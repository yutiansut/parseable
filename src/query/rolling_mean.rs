use std::any::Any;
use std::sync::Arc;
use arrow_schema::{DataType, Field};
use arrow_array::types::Float64Type;
use arrow_array::{ArrayRef, Float64Array};
use arrow_array::cast::AsArray;
use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::Float64Builder;
use datafusion::logical_expr::{WindowUDFImpl, Signature, Volatility, PartitionEvaluator};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use rayon::prelude::*;
use datafusion::common::DataFusionError;
use arrow_array::Array;


/// 滑动平均窗口函数实现
#[derive(Debug)]
pub struct RollingMeanUdf {
    signature: Signature,
}

impl RollingMeanUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64], 
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for RollingMeanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rolling_mean"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RollingMeanEvaluator::new(300))) // 默认5分钟
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, true))
    }
}

/// 滑动窗口状态管理
#[derive(Debug)]
struct RollingMeanState {
    buffer: Vec<f64>,         // 窗口缓冲区
    sum: f64,                 // 当前和
    count: usize,            // 有效值计数
}

impl RollingMeanState {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            sum: 0.0,
            count: 0,
        }
    }

    /// 添加新值并返回平均值
    fn add(&mut self, value: f64) -> f64 {
        self.buffer.push(value);
        self.sum += value;
        self.count += 1;
        
        self.sum / self.count as f64
    }

    /// 移除最老的值
    fn remove_oldest(&mut self) {
        if self.buffer.is_empty() { // 添加空缓冲区检查
            return;
        }
        let old_value = self.buffer.remove(0);
        self.sum -= old_value;
        self.count = self.count.saturating_sub(1); // 使用饱和减法
    }

    /// 维护窗口大小
    fn maintain_window_size(&mut self, window_size: usize) {
        let window_size = window_size.max(1); // 确保最小窗口大小为1
        while self.buffer.len() > window_size {
            self.remove_oldest();
        }
    }
}

#[derive(Debug)]
struct RollingMeanEvaluator {
    window_size: usize,
    state: RollingMeanState,
}

impl RollingMeanEvaluator {
    fn new(window_size: usize) -> Self {
        Self {
            window_size,
            state: RollingMeanState::new(window_size),
        }
    }
}

impl PartitionEvaluator for RollingMeanEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        let values = values[0].as_primitive::<Float64Type>();
        let window_size = self.window_size;

        // 计算整个范围的平均值
        let start = range.start;
        let end = range.end.min(values.len());
        
        let mut sum = 0.0;
        let mut count = 0;

        for i in start..end {
            if values.is_valid(i) {
                sum += values.value(i);
                count += 1;
            }
        }

        let avg = if count > 0 {
            sum / count as f64
        } else {
            f64::NAN
        };

        Ok(ScalarValue::Float64(Some(avg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use arrow::array::{Int64Array, Float64Array};
    use arrow::datatypes::Schema;
    use datafusion::logical_expr::WindowUDF;
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    /// 测试基本功能
    #[tokio::test]
    async fn test_basic_rolling_mean() -> Result<()> {
        let ctx = SessionContext::new();
        let udf = WindowUDF::from(RollingMeanUdf::new());
        ctx.register_udwf(udf);

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![data]])?;
        ctx.register_table("test", Arc::new(provider))?;

        let df = ctx.sql(
            "SELECT time, value, 
             rolling_mean(value) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
             FROM test"
        ).await?;

        let results = df.collect().await?;
        let result_array = results[0].column(2).as_primitive::<Float64Type>();

        let expected = vec![
            10.0,  // 窗口 [0]
            15.0,  // 窗口 [0,1] (10+20)/2
            20.0,  // 窗口 [0,1,2] (10+20+30)/3
            30.0,  // 窗口 [1,2,3] (20+30+40)/3
            40.0   // 窗口 [2,3,4] (30+40+50)/3
        ];

        for (i, &exp) in expected.iter().enumerate() {
            let actual = result_array.value(i);
            assert!((actual - exp).abs() < f64::EPSILON, 
                   "Index {}: Expected {}, got {}", i, exp, actual);
        }

        Ok(())
    }

    /// 测试空值处理
    #[tokio::test]
    async fn test_null_handling() -> Result<()> {
        let ctx = SessionContext::new();
        let udf = WindowUDF::from(RollingMeanUdf::new());
        ctx.register_udwf(udf);

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Float64, true), // 允许空值
        ]));

        let mut builder = Float64Array::builder(5);
        builder.append_value(10.0);
        builder.append_null();
        builder.append_value(30.0);
        builder.append_value(40.0);
        builder.append_null();

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(builder.finish()),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![data]])?;
        ctx.register_table("test", Arc::new(provider))?;

        let df = ctx.sql(
            "SELECT time, value, 
             rolling_mean(value) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
             FROM test"
        ).await?;

        let results = df.collect().await?;
        let result_array = results[0].column(2).as_primitive::<Float64Type>();

        let expected = vec![
            10.0,  // 窗口 [0] (10)
            10.0,  // 窗口 [0,1] (10 + null)
            20.0,  // 窗口 [0,1,2] (10 + null + 30) → 40/2
            35.0,  // 窗口 [1,2,3] (null + 30 + 40) → 70/2
            35.0   // 窗口 [2,3,4] (30 + 40 + null) → 70/2
        ];

        for (i, &exp) in expected.iter().enumerate() {
            let actual = result_array.value(i);
            assert!((actual - exp).abs() < f64::EPSILON, 
                   "Index {}: Expected {}, got {}", i, exp, actual);
        }

        Ok(())
    }

    /// 测试大数据量性能
    #[tokio::test]
    async fn test_large_dataset() -> Result<()> {
        let ctx = SessionContext::new();
        let udf = WindowUDF::from(RollingMeanUdf::new());
        ctx.register_udwf(udf);

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // 生成大量测试数据
        let size = 100_000;
        let times: Vec<i64> = (0..size as i64).collect();
        let values: Vec<f64> = (0..size).map(|i| i as f64).collect();

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(times)),
                Arc::new(Float64Array::from(values)),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![data]])?;
        ctx.register_table("test", Arc::new(provider))?;

        let start = std::time::Instant::now();
        let df = ctx.sql(
            "SELECT time, value, 
             rolling_mean(value) OVER (ORDER BY time ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) 
             FROM test"
        ).await?;

        let results = df.collect().await?;
        println!("Processing time: {:?}", start.elapsed());

        // 验证结果正确性
        let result_array = results[0].column(2).as_primitive::<Float64Type>();
        assert!(result_array.len() == size);

        Ok(())
    }
}
