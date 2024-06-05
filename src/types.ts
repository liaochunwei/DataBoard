/**
 * 服务端数据类型
 */
export declare type ServiceColumnType = {
    name: string,
    datatype: string,
    values: any[],
}

/**
 * 列类型
 */
export enum ColumnType { 
    /**
     * 字符串类型
     */
    String =  0,
    /**
     * 整数类型
     */
    Int = 1, 
    /**
     * 浮点类型
     */
    Float = 2,
    /**
     * 日期类型
     */
    Date = 3,
}

/**
 * 维度名称集合
 */
export declare type Dimension = {
    /**
     * 行维度名称数组
     */
    rows: string[],
    /**
     * 列维度名称数组
     */
    columns: string[],
}

/**
 * 指标计算方法
 */
export  enum MetricMode { 
    /**
     * 累和计算
     */
    "Sum" = 0, 
    /**
     * 计数计算
     */
    'Count' = 1, 
    /**
     * 最大值
     */
    'Max' = 2, 
    /**
     * 最小值
     */
    'Min' = 3, 
    /**
     * 平均值
     */
    'Avg' = 4, 
    /**
     * 总种占比
     */
    'Rate' = 5, 
}

/**
 * 指标
 */
export declare type Metric = {
    /**
     * 指标名称
     */
    index: string,
    /**
     * 指标计划方法
     */
    mode: MetricMode
}

/**
 * 计算规则
 */
export declare type Rule = {
    name: string,
    calc: string,
}

/**
 * 查询过滤方式
 */
export enum FilterMode { 
    /**
     * 单选
     */
    "Single" = 0, 
    /**
     * 多选
     */
    "Multi" = 1,
    /**
     * 前缀匹配
     */
    "MatchPrefix" = 2, 
    /**
     * 日期范围
     */
    "DateRange" = 3,
    /**
     * 数据范围
     */
    "DigitalRange" = 4
};

/**
 * 查询配置项
 */
export declare type Filter = {
    /**
     * 字段名
     */
    index: string,
    /**
     * 查询方式
     */
    mode: FilterMode
}

/**
 * 查询项
 */
export declare type SearchItem = {
    /**
     * 查询字段名
     */
    index: string,
    /**
     * 查询方式
     */
    mode: FilterMode,
    /**
     * 查询值
     */
    value: string[]
}

/**
 * 数据配置结构
 */
export declare interface ISetting {
    /**
     * 数据列配置信息
     */
    columns: Map<string, ColumnType>,
    /**
     * 数据查询维度配置
     */
    dimensions: Dimension,
    /**
     * 数据查询指标配置
     */
    metrics: Metric[],
    /**
     * 数据查询过滤配置
     */
    filters: Filter[],
    /**
     * 数据查询附加计算规则配置
     */
    rules: Rule[],
    /**
     * 配置是否已确认
     */
    active: boolean,
}
