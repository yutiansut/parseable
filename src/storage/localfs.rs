/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use fs_extra::file::CopyOptions;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::fs::{self, DirEntry};
use tokio_stream::wrappers::ReadDirStream;

use crate::option::validation;
use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metrics::storage::{localfs::REQUEST_RESPONSE_TIME, StorageMetrics},
};

use super::{
    LogStream, ObjectStorage, ObjectStorageError, ObjectStorageProvider, ALERTS_ROOT_DIRECTORY,
    PARSEABLE_ROOT_DIRECTORY, SCHEMA_FILE_NAME, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
};

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "Local filesystem config",
    about = "Start Parseable with a drive as storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct FSConfig {
    #[arg(
        env = "P_FS_DIR",
        value_name = "filesystem path",
        default_value = "./data",
        value_parser = validation::canonicalize_path
    )]
    pub root: PathBuf,
}

impl ObjectStorageProvider for FSConfig {
    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        RuntimeEnvBuilder::new()
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        Arc::new(LocalFS::new(self.root.clone()))
    }

    fn get_endpoint(&self) -> String {
        self.root.to_str().unwrap().to_string()
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler);
    }
}

/// 本地文件系统存储实现
/// 实现 ObjectStorage trait 用于对接 Parseable 存储抽象层
#[derive(Debug)]
pub struct LocalFS {
    /// 数据存储根目录的绝对路径
    /// 示例：PathBuf::from("/var/lib/parseable/data")
    root: PathBuf,
}

impl LocalFS {
    /// 创建新的 LocalFS 实例
    /// 参数：
    /// - root: 数据存储根目录路径
    /// 示例：LocalFS::new(PathBuf::from("/data"))
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// 将相对路径转换为绝对路径
    /// 参数：
    /// - path: 相对路径（基于存储根目录）
    /// 返回：组合后的绝对路径
    /// 示例：path_in_root("logs/nginx") → /data/logs/nginx
    pub fn path_in_root(&self, path: &RelativePath) -> PathBuf {
        path.to_path(&self.root)
    }
}

/// 实现 ObjectStorage trait 的核心方法
#[async_trait]
impl ObjectStorage for LocalFS {
    /// 获取指定路径的文件内容
    /// 参数：
    /// - path: 相对路径（如 "logs/2023/01/01/0001.parquet"）
    /// 返回：Bytes 文件内容 或 错误
    /// 指标：记录请求响应时间和状态码（404/500等）
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let time = Instant::now();
        let file_path = self.path_in_root(path);
        let res: Result<Bytes, ObjectStorageError> = match fs::read(file_path).await {
            Ok(x) => Ok(x.into()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    Err(ObjectStorageError::NoSuchKey(path.to_string()))
                }
                _ => Err(ObjectStorageError::UnhandledError(Box::new(e))),
            },
        };

        let status = if res.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", status])
            .observe(time);
        res
    }

    /// 获取所有包含 "ingestor" 的元数据文件路径
    /// 返回：相对路径列表（如 ["ingestor_meta_v1.json"]）
    /// 用途：用于系统恢复和元数据管理
    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();

        let mut path_arr = vec![];
        let mut entries = fs::read_dir(&self.root).await?;

        while let Some(entry) = entries.next_entry().await? {
            let flag = entry
                .path()
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
                .contains("ingestor");

            if flag {
                path_arr.push(
                    RelativePathBuf::from_path(entry.path().file_name().unwrap())
                        .map_err(ObjectStorageError::PathError)?,
                );
            }
        }

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"]) // this might not be the right status code
            .observe(time);

        Ok(path_arr)
    }

    /// 获取指定流的所有相关文件路径
    /// 参数：
    /// - stream_name: 日志流名称（如 "nginx"）
    /// 返回：包含以下文件的路径列表：
    ///   - 数据文件（含 "ingestor" 标记）
    ///   - 流元数据文件（stream.json）
    ///   - 模式文件（schema.json）
    async fn get_stream_file_paths(
        &self,
        stream_name: &str,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();
        let mut path_arr = vec![];

        // = data/stream_name
        let stream_dir_path = self.path_in_root(&RelativePathBuf::from(stream_name));
        let mut entries = fs::read_dir(&stream_dir_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let flag = entry
                .path()
                .file_name()
                .ok_or(ObjectStorageError::NoSuchKey(
                    "Dir Entry Suggests no file present".to_string(),
                ))?
                .to_str()
                .expect("file name is parseable to str")
                .contains("ingestor");

            if flag {
                path_arr.push(RelativePathBuf::from_iter([
                    stream_name,
                    entry.path().file_name().unwrap().to_str().unwrap(), // checking the error before hand
                ]));
            }
        }

        path_arr.push(RelativePathBuf::from_iter([
            stream_name,
            STREAM_METADATA_FILE_NAME,
        ]));
        path_arr.push(RelativePathBuf::from_iter([stream_name, SCHEMA_FILE_NAME]));

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"]) // this might not be the right status code
            .observe(time);

        Ok(path_arr)
    }

    /// 根据过滤条件获取多个对象数据
    /// 参数：
    /// - base_path: 基础路径（可选，用于限定搜索范围）
    /// - filter_func: 过滤函数（判断文件名是否符合条件）
    /// 返回：符合条件文件的内容列表
    /// 注意：用于批量获取ingestor元数据文件
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<(dyn Fn(String) -> bool + std::marker::Send + 'static)>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let time = Instant::now();

        let prefix = if let Some(path) = base_path {
            path.to_path(&self.root)
        } else {
            self.root.clone()
        };

        let mut entries = fs::read_dir(&prefix).await?;
        let mut res = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry
                .path()
                .file_name()
                .ok_or(ObjectStorageError::NoSuchKey(
                    "Dir Entry suggests no file present".to_string(),
                ))?
                .to_str()
                .expect("file name is parseable to str")
                .to_owned();
            let ingestor_file = filter_func(path);

            if !ingestor_file {
                continue;
            }

            let file = fs::read(entry.path()).await?;
            res.push(file.into());
        }

        // maybe change the return code
        let status = if res.is_empty() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", status])
            .observe(time);

        Ok(res)
    }

    /// 写入对象到指定路径
    /// 参数：
    /// - path: 目标相对路径
    /// - resource: 要写入的字节数据
    /// 功能：
    /// 1. 自动创建父目录
    /// 2. 原子写入文件
    /// 3. 记录性能指标
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        let time = Instant::now();

        let path = self.path_in_root(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let res = fs::write(path, resource).await;

        let status = if res.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", status])
            .observe(time);

        res.map_err(Into::into)
    }

    /// 删除指定前缀（目录）下的所有内容
    /// 参数：
    /// - path: 要删除的目录相对路径
    /// 注意：递归删除目录及其所有子内容
    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    /// 删除单个文件对象
    /// 参数：
    /// - path: 要删除的文件相对路径
    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    /// 存储系统健康检查
    /// 功能：
    /// 1. 确保根目录存在
    /// 2. 验证目录可写性
    async fn check(&self) -> Result<(), ObjectStorageError> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|e| ObjectStorageError::UnhandledError(e.into()))
    }

    /// 删除指定日志流的所有数据
    /// 参数：
    /// - stream_name: 要删除的流名称
    /// 注意：实际删除对应的流目录
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let path = self.root.join(stream_name);
        Ok(fs::remove_dir_all(path).await?)
    }
    async fn try_delete_ingestor_meta(
        &self,
        ingestor_filename: String,
    ) -> Result<(), ObjectStorageError> {
        let path = self.root.join(ingestor_filename);
        Ok(fs::remove_file(path).await?)
    }
    /// 列出所有有效日志流
    /// 过滤目录：
    /// - "lost+found"：系统保留目录
    /// - 其他Parseable系统目录（如用户、告警目录）
    /// 返回：LogStream 的集合
    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let ignore_dir = &[
            "lost+found",
            PARSEABLE_ROOT_DIRECTORY,
            USERS_ROOT_DIR,
            ALERTS_ROOT_DIRECTORY,
        ];
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries
            .into_iter()
            .map(|entry| dir_with_stream(entry, ignore_dir));

        let logstream_dirs: Vec<Option<String>> =
            FuturesUnordered::from_iter(entries).try_collect().await?;

        let logstreams = logstream_dirs.into_iter().flatten().collect();

        Ok(logstreams)
    }

    /// 列出旧版日志流（兼容性处理）
    /// 过滤目录与 list_streams 略有不同
    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let ignore_dir = &[
            "lost+found",
            PARSEABLE_ROOT_DIRECTORY,
            ALERTS_ROOT_DIRECTORY,
        ];
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries
            .into_iter()
            .map(|entry| dir_with_old_stream(entry, ignore_dir));

        let logstream_dirs: Vec<Option<String>> =
            FuturesUnordered::from_iter(entries).try_collect().await?;

        let logstreams = logstream_dirs.into_iter().flatten().collect();

        Ok(logstreams)
    }

    /// 列出根目录下的所有一级子目录
    /// 返回：目录名称列表（字符串形式）
    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError> {
        let dirs = ReadDirStream::new(fs::read_dir(&self.root).await?)
            .try_collect::<Vec<DirEntry>>()
            .await?
            .into_iter()
            .map(dir_name);

        let dirs = FuturesUnordered::from_iter(dirs)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(dirs)
    }

    /// 获取所有用户的仪表板配置
    /// 返回：HashMap<相对路径, 文件内容列表>
    /// 路径结构：users/{username}/dashboards/{dashboard_name}.json
    async fn get_all_dashboards(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut dashboards: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = self.root.join(USERS_ROOT_DIR);
        let directories = ReadDirStream::new(fs::read_dir(&users_root_path).await?);
        let users: Vec<DirEntry> = directories.try_collect().await?;
        for user in users {
            if !user.path().is_dir() {
                continue;
            }
            let dashboards_path = users_root_path.join(user.path()).join("dashboards");
            let directories = ReadDirStream::new(fs::read_dir(&dashboards_path).await?);
            let dashboards_files: Vec<DirEntry> = directories.try_collect().await?;
            for dashboard in dashboards_files {
                let dashboard_absolute_path = dashboard.path();
                let file = fs::read(dashboard_absolute_path.clone()).await?;
                let dashboard_relative_path = dashboard_absolute_path
                    .strip_prefix(self.root.as_path())
                    .unwrap();

                dashboards
                    .entry(RelativePathBuf::from_path(dashboard_relative_path).unwrap())
                    .or_default()
                    .push(file.into());
            }
        }
        Ok(dashboards)
    }

    /// 获取所有保存的过滤器配置
    /// 返回：HashMap<相对路径, 文件内容列表>
    /// 路径结构：users/{username}/filters/{stream_name}/{filter_name}.json
    async fn get_all_saved_filters(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut filters: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = self.root.join(USERS_ROOT_DIR);
        let directories = ReadDirStream::new(fs::read_dir(&users_root_path).await?);
        let users: Vec<DirEntry> = directories.try_collect().await?;
        for user in users {
            if !user.path().is_dir() {
                continue;
            }
            let stream_root_path = users_root_path.join(user.path()).join("filters");
            let directories = ReadDirStream::new(fs::read_dir(&stream_root_path).await?);
            let streams: Vec<DirEntry> = directories.try_collect().await?;
            for stream in streams {
                if !stream.path().is_dir() {
                    continue;
                }
                let filters_path = users_root_path
                    .join(user.path())
                    .join("filters")
                    .join(stream.path());
                let directories = ReadDirStream::new(fs::read_dir(&filters_path).await?);
                let filters_files: Vec<DirEntry> = directories.try_collect().await?;
                for filter in filters_files {
                    let filter_absolute_path = filter.path();
                    let file = fs::read(filter_absolute_path.clone()).await?;
                    let filter_relative_path = filter_absolute_path
                        .strip_prefix(self.root.as_path())
                        .unwrap();

                    filters
                        .entry(RelativePathBuf::from_path(filter_relative_path).unwrap())
                        .or_default()
                        .push(file.into());
                }
            }
        }
        Ok(filters)
    }

    ///fetch all correlations stored in disk
    /// return the correlation file path and all correlation json bytes for each file path
    async fn get_all_correlations(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut correlations: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = self.root.join(USERS_ROOT_DIR);
        let mut directories = ReadDirStream::new(fs::read_dir(&users_root_path).await?);
        while let Some(user) = directories.next().await {
            let user = user?;
            if !user.path().is_dir() {
                continue;
            }
            let correlations_path = users_root_path.join(user.path()).join("correlations");
            let mut files = ReadDirStream::new(fs::read_dir(&correlations_path).await?);
            while let Some(correlation) = files.next().await {
                let correlation_absolute_path = correlation?.path();
                let file = fs::read(correlation_absolute_path.clone()).await?;
                let correlation_relative_path = correlation_absolute_path
                    .strip_prefix(self.root.as_path())
                    .unwrap();

                correlations
                    .entry(RelativePathBuf::from_path(correlation_relative_path).unwrap())
                    .or_default()
                    .push(file.into());
            }
        }
        Ok(correlations)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let path = self.root.join(stream_name);
        let directories = ReadDirStream::new(fs::read_dir(&path).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let dates: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;

        Ok(dates.into_iter().flatten().collect())
    }

    async fn list_manifest_files(
        &self,
        _stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError> {
        //unimplemented
        Ok(BTreeMap::new())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        let op = CopyOptions {
            overwrite: true,
            skip_exist: true,
            ..CopyOptions::default()
        };
        let to_path = self.root.join(key);
        if let Some(path) = to_path.parent() {
            fs::create_dir_all(path).await?;
        }
        let _ = fs_extra::file::copy(path, to_path, &op)?;
        Ok(())
    }

    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path {
        object_store::path::Path::parse(
            format!("{}", self.root.join(prefix.as_str()).display())
                .trim_start_matches(std::path::MAIN_SEPARATOR),
        )
        .unwrap()
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl> {
        prefixes
            .into_iter()
            .filter_map(|prefix| ListingTableUrl::parse(format!("/{}", prefix)).ok())
            .collect()
    }

    fn store_url(&self) -> url::Url {
        url::Url::parse("file:///").unwrap()
    }

    fn get_bucket_name(&self) -> String {
        self.root
            .iter()
            .last()
            .expect("can be unwrapped without checking as the path is absolute")
            .to_str()
            .expect("valid unicode")
            .to_string()
    }
}

async fn dir_with_old_stream(
    entry: DirEntry,
    ignore_dirs: &[&str],
) -> Result<Option<String>, ObjectStorageError> {
    let dir_name = entry
        .path()
        .file_name()
        .expect("valid path")
        .to_str()
        .expect("valid unicode")
        .to_owned();

    if ignore_dirs.contains(&dir_name.as_str()) {
        return Ok(None);
    }

    if entry.file_type().await?.is_dir() {
        let path = entry.path();

        // even in ingest mode, we should only look for the global stream metadata file
        let stream_json_path = path.join(STREAM_METADATA_FILE_NAME);

        if stream_json_path.exists() {
            Ok(Some(dir_name))
        } else {
            let err: Box<dyn std::error::Error + Send + Sync + 'static> =
                format!("found {}", entry.path().display()).into();
            Err(ObjectStorageError::UnhandledError(err))
        }
    } else {
        Ok(None)
    }
}

async fn dir_with_stream(
    entry: DirEntry,
    ignore_dirs: &[&str],
) -> Result<Option<String>, ObjectStorageError> {
    let dir_name = entry
        .path()
        .file_name()
        .expect("valid path")
        .to_str()
        .expect("valid unicode")
        .to_owned();

    if ignore_dirs.contains(&dir_name.as_str()) {
        return Ok(None);
    }

    if entry.file_type().await?.is_dir() {
        let path = entry.path();

        // even in ingest mode, we should only look for the global stream metadata file
        let stream_json_path = path
            .join(STREAM_ROOT_DIRECTORY)
            .join(STREAM_METADATA_FILE_NAME);

        if stream_json_path.exists() {
            Ok(Some(dir_name))
        } else {
            let err: Box<dyn std::error::Error + Send + Sync + 'static> =
                format!("found {}", entry.path().display()).into();
            Err(ObjectStorageError::UnhandledError(err))
        }
    } else {
        Ok(None)
    }
}

async fn dir_name(entry: DirEntry) -> Result<Option<String>, ObjectStorageError> {
    if entry.file_type().await?.is_dir() {
        let dir_name = entry
            .path()
            .file_name()
            .expect("valid path")
            .to_str()
            .expect("valid unicode")
            .to_owned();
        Ok(Some(dir_name))
    } else {
        Ok(None)
    }
}

impl From<fs_extra::error::Error> for ObjectStorageError {
    fn from(e: fs_extra::error::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(e))
    }
}
