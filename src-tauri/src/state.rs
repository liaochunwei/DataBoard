use regex::Regex;

use polars::lazy::dsl::{col, Expr};
use polars::prelude::*;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use std::{
    collections::HashMap,
    format,
    io::{BufWriter, Write as _},
};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ColumnType {
    String = 0,
    Integer = 1,
    Float = 2,
    Date = 3,
}
impl Serialize for ColumnType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(match value {
            1 => ColumnType::Integer,
            2 => ColumnType::Float,
            3 => ColumnType::Date,
            _ => ColumnType::String,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Dimension {
    pub rows: Vec<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MetricMode {
    Sum = 0,
    Count = 1,
    Max = 2,
    Min = 3,
    Avg = 4,
    Rate = 5,
}
impl Serialize for MetricMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = *self as i32;
        serializer.serialize_i32(value)
    }
}

impl<'de> Deserialize<'de> for MetricMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(match value {
            0 => MetricMode::Sum,
            1 => MetricMode::Count,
            2 => MetricMode::Max,
            3 => MetricMode::Min,
            4 => MetricMode::Avg,
            5 => MetricMode::Rate,
            _ => MetricMode::Count,
        })
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum FilterMode {
    Single = 0,
    Multi = 1,
    MatchPrefix = 2,
    DateRange = 3,
    DigitalRange = 4,
}
impl Serialize for FilterMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = *self as i32;
        serializer.serialize_i32(value)
    }
}

impl<'de> Deserialize<'de> for FilterMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(match value {
            0 => FilterMode::Single,
            1 => FilterMode::Multi,
            2 => FilterMode::MatchPrefix,
            3 => FilterMode::DateRange,
            4 => FilterMode::DigitalRange,
            _ => FilterMode::MatchPrefix,
        })
    }
}
#[derive(Serialize, Deserialize)]
pub struct Metric {
    pub index: String,
    pub mode: MetricMode,
}

#[derive(Serialize, Deserialize)]
pub struct Filter {
    pub index: String,
    pub mode: FilterMode,
}
#[derive(Serialize, Deserialize)]
pub struct FilterItem {
    pub index: String,
    pub mode: FilterMode,
    pub value: Vec<String>,
}
#[derive(Serialize, Deserialize)]
pub struct Rule {
    pub name: String,
    pub calc: String,
}

#[derive(Serialize, Deserialize)]
pub struct Setting {
    pub columns: HashMap<String, ColumnType>,
}

#[derive(Serialize, Deserialize)]
pub struct Query {
    pub dimensions: Dimension,
    pub metrics: Vec<Metric>,
    pub filters: Vec<Filter>,
    pub rules: Vec<Rule>,
    pub search: Vec<FilterItem>,
}

pub struct StateStore {
    // 原始数据
    records: DataFrame,
    // 标准化数据
    standard: DataFrame,
    // 当前查询数据
    result: DataFrame,
}
impl StateStore {
    pub fn default() -> StateStore {
        return StateStore {
            records: DataFrame::default(),
            standard: DataFrame::default(),
            result: DataFrame::default(),
        };
    }
    // 读CSV文件
    pub fn read_csv(&mut self, path: &str) -> bool {
        self.records = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(path.into()))
            .unwrap()
            .finish()
            .unwrap();
        log::debug!("{}", &self.records.head(Some(5)));
        true
    }

    // 保存CSV文件
    pub fn save_csv(&mut self, path: &str) -> bool {
        let mut df = self.result.clone();
        let mut file = std::fs::File::create(path).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
        true
    }

    // 标准数据行数
    pub fn count(&mut self) -> usize {
        self.records.shape().0
    }

    // 标准数据的列
    pub fn columns(&mut self) -> Vec<Series> {
        let rdf = self.records.head(Some(1));
        let ret = rdf.get_columns();
        ret.to_vec()
    }
    // 获取指定列的唯一值
    pub fn column_unique(&mut self, name: String) -> Series {
        let ret = self.standard.column(&name).unwrap().unique().unwrap();
        ret
    }

    // 预览标准数据行数
    pub fn preview(&mut self, count: usize) -> DataFrame {
        self.records.head(Some(count))
    }

    // 数据格式标准化
    pub fn etl(&mut self, mapping: HashMap<String, ColumnType>) -> Result<(), PolarsError> {
        let mut new_df = DataFrame::default();

        for (_index, s) in self.records.get_columns().iter().enumerate() {
            let o_cast = mapping.get(s.name());
            match o_cast {
                Some(ctype) => {
                    let transform = match s.dtype() {
                        DataType::Int32 => match ctype {
                            ColumnType::String => s.cast(&DataType::String),
                            ColumnType::Float => s.cast(&DataType::Float32),
                            ColumnType::Date => {
                                (s * 1_000).cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                            }
                            _ => s.cast(s.dtype()),
                        },
                        DataType::Float32 | DataType::Float64 => match ctype {
                            ColumnType::String => s.cast(&DataType::String),
                            ColumnType::Integer => s.cast(&DataType::Int32),
                            ColumnType::Float => s.cast(&DataType::Float32),
                            ColumnType::Date => (s * 1_000)
                                .cast(&DataType::Int32)?
                                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None)),
                        },
                        DataType::String => match ctype {
                            ColumnType::String => s
                                .str()?
                                .into_iter()
                                .map(|op| op.map(|val| val.trim()))
                                .collect::<StringChunked>()
                                .cast(&DataType::String),
                            ColumnType::Integer => {
                                let re = Regex::new(r"\d+").unwrap();
                                s.str()?
                                    .into_iter()
                                    .map(|op| {
                                        op.and_then(|val| {
                                            re.find(val)
                                                .and_then(|v| v.as_str().parse::<i32>().ok())
                                        })
                                    })
                                    .collect::<Int32Chunked>()
                                    .cast(&DataType::Int32)
                            }
                            ColumnType::Float => {
                                let re = Regex::new(r"\d+[\,\.\d]*").unwrap();
                                s.str()?
                                    .into_iter()
                                    .map(|op| {
                                        op.and_then(|val| {
                                            re.find(val).and_then(|v| {
                                                let mut s = v.as_str().replace(",", "");
                                                if s.ends_with('.') {
                                                    s = format!("{}0", s)
                                                }
                                                s.parse::<f32>().ok()
                                            })
                                        })
                                    })
                                    .collect::<Float32Chunked>()
                                    .cast(&DataType::Float32)
                            }
                            ColumnType::Date => {
                                let re_00 =
                                    Regex::new(r"\b(\d{4})[-.](\d{2})[-.](\d{2})\b").unwrap(); //yyyy-mm-dd yyyy.mm.dd
                                let re_01 = Regex::new(r"\b(\d{4})(\d{2})(\d{2})\b").unwrap(); //yyyymmdd
                                let re_02 = Regex::new(r"\b(\d{2})(\d{2})(\d{2})\b").unwrap(); //yymmdd
                                let re_03 = Regex::new(r"\b(\d{4})/(\d{2})/(\d{2})\b").unwrap(); // yyyy/mm/dd
                                let re_04 = Regex::new(r"\b(\d{2})/(\d{2})/(\d{4})\b").unwrap(); // dd/mm/yyyy
                                let re_05 = Regex::new(r"\b(\d{4})年(\d{2})月(\d{2})日\b").unwrap(); // yyyy年mm月dd日

                                fn format_date(year: &str, month: &str, day: &str) -> String {
                                    format!("{}-{}-{}", year, month, day)
                                }

                                s.str()?
                                    .into_iter()
                                    .map(|op| {
                                        op.and_then(|val| {
                                            if let Some(caps) = re_00.captures(val) {
                                                return Some(format_date(
                                                    &caps[1], &caps[2], &caps[3],
                                                ));
                                            } else if let Some(caps) = re_01.captures(val) {
                                                return Some(format_date(
                                                    &caps[1], &caps[2], &caps[3],
                                                ));
                                            } else if let Some(caps) = re_02.captures(val) {
                                                return Some(format!(
                                                    "20{}",
                                                    format_date(&caps[1], &caps[2], &caps[3])
                                                ));
                                            } else if let Some(caps) = re_03.captures(val) {
                                                return Some(format_date(
                                                    &caps[1], &caps[2], &caps[3],
                                                ));
                                            } else if let Some(caps) = re_04.captures(val) {
                                                return Some(format_date(
                                                    &caps[3], &caps[2], &caps[1],
                                                ));
                                            } else if let Some(caps) = re_05.captures(val) {
                                                return Some(format_date(
                                                    &caps[1], &caps[2], &caps[3],
                                                ));
                                            } else {
                                                return Some("2000-01-01".to_string());
                                            }
                                        })
                                    })
                                    .collect::<StringChunked>()
                                    .as_date(Some("%Y-%m-%d"), true)?
                                    .cast(&DataType::Date)
                            }
                        },
                        DataType::Date => match ctype {
                            ColumnType::String => s.cast(&DataType::String),
                            ColumnType::Integer => s.cast(&DataType::Int32),
                            ColumnType::Float => s.cast(&DataType::Float32),
                            _ => s.cast(s.dtype()),
                        },
                        DataType::Boolean => match ctype {
                            ColumnType::String => s.cast(&DataType::String),
                            ColumnType::Integer => s.cast(&DataType::Int8),
                            ColumnType::Float => s.cast(&DataType::Float32),
                            _ => s.cast(s.dtype()),
                        },
                        _ => match ctype {
                            ColumnType::String => s.cast(&DataType::String),
                            ColumnType::Integer => s.cast(&DataType::Int32),
                            ColumnType::Float => s.cast(&DataType::Float32),
                            _ => s.cast(s.dtype()),
                        },
                    };
                    match transform {
                        Ok(mut t) => {
                            let col = t.rename(s.name());
                            match new_df.with_column(col.clone()) {
                                Ok(r) => new_df = r.clone(),
                                Err(_) => todo!(),
                            }
                        }
                        Err(_) => todo!(),
                    }
                }
                None => {
                    return Err(PolarsError::InvalidOperation("".into()));
                }
            }
        }
        self.standard = new_df;
        Ok(())
    }

    // 数据处理
    pub fn search(&mut self, query: Query) -> DataFrame {
        let mut df = self.standard.clone();

        let custom_filter = |cdf: &DataFrame| {
            //第一列数据不能为空
            let mut filter = cdf.get_columns()[0].is_not_null();

            for ele in query.search {
                let sr = cdf.column(ele.index.as_str());
                match sr {
                    Ok(s) => match s.dtype() {
                        DataType::Int32 => {
                            let vr: Result<Vec<i32>, _> =
                                ele.value.into_iter().map(|s| s.parse::<i32>()).collect();
                            match vr {
                                Ok(v) => {
                                    if v.len() == 0 {
                                        continue;
                                    }
                                    match ele.mode {
                                        FilterMode::Single | FilterMode::MatchPrefix => {
                                            let m = v.get(0).unwrap();
                                            let cp = s.equal(*m);

                                            filter = filter & (cp.unwrap());
                                        }
                                        FilterMode::Multi => {
                                            let vs: Result<ChunkedArray<BooleanType>, PolarsError> =
                                                is_in(s, &Series::new("cm", v));
                                            match vs {
                                                Ok(s) => {
                                                    filter = filter & s;
                                                }
                                                Err(_) => {}
                                            }
                                        }
                                        FilterMode::DateRange => {
                                            if v.len() > 1 {
                                                let sv = v.get(0).unwrap();
                                                let ev = v.get(1).unwrap();
                                                let cp =
                                                    s.gt_eq(*sv).unwrap() & s.lt_eq(*ev).unwrap();
                                                filter = filter & cp;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                        DataType::Float32 => {
                            let vr: Result<Vec<f32>, _> =
                                ele.value.into_iter().map(|s| s.parse::<f32>()).collect();
                            match vr {
                                Ok(v) => {
                                    if v.len() == 0 {
                                        continue;
                                    }
                                    match ele.mode {
                                        FilterMode::DigitalRange => {
                                            if v.len() > 1 {
                                                let sv = v.get(0).unwrap();
                                                let ev = v.get(1).unwrap();
                                                let cp =
                                                    s.gt_eq(*sv).unwrap() & s.lt_eq(*ev).unwrap();
                                                filter = filter & cp;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                        DataType::String => {
                            let v: Vec<String> = ele
                                .value
                                .into_iter()
                                .map(|s| s.trim().to_string())
                                .collect();

                            if v.len() == 0 {
                                continue;
                            }
                            match ele.mode {
                                FilterMode::Single => {
                                    let m = v.get(0).unwrap();
                                    let r = s
                                        .str()
                                        .unwrap()
                                        .into_iter()
                                        .map(|v| {
                                            v.and_then(|v| {
                                                let cp = v.eq(m);
                                                Some(cp)
                                            })
                                        })
                                        .collect::<BooleanChunked>();
                                    filter = filter & r;
                                }
                                FilterMode::Multi => {
                                    let vs: Result<ChunkedArray<BooleanType>, PolarsError> =
                                        is_in(s, &Series::new("cm", v));
                                    match vs {
                                        Ok(s) => {
                                            filter = filter & s;
                                        }
                                        Err(_) => {}
                                    }
                                }
                                FilterMode::MatchPrefix => {
                                    let m = v.get(0).unwrap();
                                    let r = s
                                        .str()
                                        .unwrap()
                                        .into_iter()
                                        .map(|v| {
                                            v.and_then(|v| {
                                                let cp = v.starts_with(m.as_str());
                                                Some(cp)
                                            })
                                        })
                                        .collect::<BooleanChunked>();
                                    filter = filter & r;
                                }
                                _ => {}
                            }
                        }
                        DataType::Date => {
                            let v: Vec<String> = ele
                                .value
                                .into_iter()
                                .map(|s| s.trim().to_string())
                                .collect();
                            let sv = s.date().unwrap().strftime("%Y-%m-%d");
                            if v.len() == 0 {
                                continue;
                            }
                            match ele.mode {
                                FilterMode::Single => {
                                    let m = v.get(0).unwrap();
                                    let r = sv
                                        .into_iter()
                                        .map(|v| {
                                            v.and_then(|v| {
                                                let cp = v.eq(m);
                                                Some(cp)
                                            })
                                        })
                                        .collect::<BooleanChunked>();
                                    filter = filter & r;
                                }
                                FilterMode::Multi => {
                                    let vs: Result<ChunkedArray<BooleanType>, PolarsError> =
                                        is_in(&sv.into_series(), &Series::new("cm", v));
                                    match vs {
                                        Ok(s) => {
                                            filter = filter & s;
                                        }
                                        Err(_) => {}
                                    }
                                }
                                FilterMode::MatchPrefix => {
                                    let sv = v.get(0).unwrap();
                                    let ev = v.get(1).unwrap();
                                    let arg = DateChunked::parse_from_str_slice(
                                        "dt",
                                        &[sv, ev],
                                        "%Y-%m-%d",
                                    );
                                    let isv = arg.get(0).unwrap();
                                    let iev = arg.get(1).unwrap();

                                    let cp = s.gt_eq(isv).unwrap() & s.lt_eq(iev).unwrap();
                                    filter = filter & cp;
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    },
                    Err(_) => {}
                }
            }
            filter
        };

        df = df.filter(&custom_filter(&df)).unwrap();
        // todo:透视表暂只支持单列单行单值
        if query.dimensions.columns.len() > 0 {
            if query.dimensions.rows.len() > 0 {
                let d_row = &query.dimensions.rows[0..1];
                let d_col = &query.dimensions.columns[0..1];
                for ele in query.metrics {
                    let epx: Expr;
                    match ele.mode {
                        MetricMode::Sum => {
                            epx = col(&ele.index).sum();
                        }
                        MetricMode::Max => {
                            epx = col(&ele.index).max();
                        }
                        MetricMode::Min => {
                            epx = col(&ele.index).min();
                        }
                        MetricMode::Avg => {
                            epx = col(&ele.index).mean();
                        }
                        MetricMode::Rate => todo!(),
                        _ => {
                            epx = col(&ele.index).count();
                        }
                    }
                    let pivot_df = pivot::pivot(
                        &df,
                        d_row,
                        d_col,
                        Some(vec![ele.index]),
                        false,
                        Some(epx),
                        Option::default(),
                    );
                    match pivot_df {
                        Ok(v) => {
                            df = v
                                .sort(
                                    d_row,
                                    SortMultipleOptions::new().with_order_descending(false),
                                )
                                .unwrap();
                            break;
                        }
                        Err(_) => {}
                    }
                }
            }
        }
        // 聚合表
        else if query.dimensions.rows.len() > 0 {
            let mut dims = Vec::<Expr>::default();
            let mut sorts = Vec::<String>::default();
            for ele in query.dimensions.rows {
                dims.push(col(&ele));
                sorts.push(ele);
            }

            let mut aggs = Vec::<Expr>::default();
            for ele in query.metrics {
                let epx: Expr;
                match ele.mode {
                    MetricMode::Sum => {
                        epx = col(&ele.index).sum();
                    }
                    MetricMode::Max => {
                        epx = col(&ele.index).max();
                    }
                    MetricMode::Min => {
                        epx = col(&ele.index).min();
                    }
                    MetricMode::Avg => {
                        epx = col(&ele.index).mean();
                    }
                    MetricMode::Rate => todo!(),
                    _ => {
                        epx = col(&ele.index).count();
                    }
                }
                aggs.push(epx);
            }
            df = df
                .lazy()
                .group_by(dims)
                .agg(aggs)
                .sort(
                    sorts,
                    SortMultipleOptions::new().with_order_descending(false),
                )
                .collect()
                .unwrap();
        }

        log::debug!("{}", &df);
        self.result = df;
        self.result.clone().head(Some(30))
    }

    // 获取结果数据的指定行集
    pub fn records(&mut self, start: i64, limit: usize) -> DataFrame {
        self.result.slice(start, limit)
    }

    // 数据转化为JSON字符串
    pub fn to_string(&mut self, df: &mut DataFrame) -> String {
        let mut buf: BufWriter<Vec<u8>> = BufWriter::new(Vec::new());
        let mut write = JsonWriter::new(buf.by_ref()).with_json_format(JsonFormat::Json);
        let _ = write.finish(df);
        String::from_utf8(buf.into_inner().unwrap()).unwrap()
    }
}
