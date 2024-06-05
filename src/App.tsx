import "./App.css";
import "@arco-design/web-react/dist/css/arco.css";

import { Window, getCurrent } from '@tauri-apps/api/window';

import * as utils from "./utils"

import { useEffect, useRef, useState } from "react";

import { Drawer, Button, Space, Select, List, Collapse, Divider, Layout, Spin, Input, Notification, DatePicker } from "@arco-design/web-react";
import { IconClose, IconPlus, IconSettings, IconShareExternal } from '@arco-design/web-react/icon';

import * as ReactVTable from '@visactor/react-vtable';
import { Services } from "./utils";
import { ISetting, ColumnType, SearchItem, ServiceColumnType, Filter, FilterMode, MetricMode, Metric } from "./types";

declare interface IDataset {
    /**
     * 原始数据文件
     */
    csv?: string;
    /**
     * 是否加载中
     */
    loading: boolean;
    /**
     * 数据处理中
     */
    reading: boolean;
    /**
     * 数据列
     */
    columns?: Array<any>;
    /**
     * 数据集
     */
    records?: Array<any>;
}

const DatasetDefault: IDataset = {
    loading: false,
    reading: false,
    records: []
}

function App() {
    const [main, _] = useState<Window>(getCurrent());
    const [version, setVersion] = useState("-");
    const [visible, setVisible] = useState(false);
    const refWrapper = useRef(null);

    const [dataset, setDataset] = useState<IDataset>(DatasetDefault);
    const [setting, setSetting] = useState<ISetting>({ columns: new Map<string, ColumnType>(), dimensions: { rows: [], columns: [] }, metrics: [], filters: [], rules: [], active: false });
    const [search, setSearch] = useState<SearchItem[]>([]);

    const [colValues, setColValues] = useState(new Map<string, string[]>);

    const [colCfg, setColCfg] = useState<any[]>([]);

    useEffect(() => {
        (async () => {
            setVersion(await utils.getVersion());
            await main.setTitle(`数据表-v${version} ${dataset.csv??''}`);
        })();
    });

    // 打开CSV文本文件并做初始配置
    async function openCsv() {
        let file = await utils.dialog.open({ multiple: false, defaultPath: await utils.path.appLocalDataDir(), filters: [{ name: "CSV 文本数据", extensions: ["csv"] }] })
        if (file) {
            setDataset({ ...dataset, loading: true });
            let ret = await Services.load(file.path);
            if (ret) {
                // 源数据列
                let columns: ServiceColumnType[] = await Services.columns();

                // 预览数据
                let records: any[] = await Services.preview();

                // 加载数据完成
                setDataset({ ...dataset, csv: file.path, loading: false, records, columns: columns.map(x => x.name) })

                // 简单数据类型推断
                let col = columns.map((x: ServiceColumnType) => {
                    return {
                        field: x.name,
                        title: x.name,
                        width: 'auto'
                    }
                })
                let types = (t: string, x: any) => {
                    if (t == "Int64" && x > 2147483647) {
                        x = String(x)
                    }
                    if (typeof (x) == 'string') {
                        x = x.trim()
                        if (x.length >= 8) {
                            let d = x.match(/^(\d{2}|\d{4})[-年\.\/]*(\d{2})[-\.\/月]*(\d{2})[日]*$/)
                            if (d) {
                                return ColumnType.Date
                            }
                        }
                        if (x.length > 9) {
                            return ColumnType.String
                        }
                        let y = x.match(/^[0-9]+$/);
                        if (y) {
                            let v = parseInt(y[0]);
                            if (!Number.isNaN(v)) {
                                return ColumnType.Int
                            }
                        }
                        let f = x.match(/^[\.0-9%]+$/);
                        if (f) {
                            let v = parseFloat(f[0]);
                            if (!Number.isNaN(v)) {
                                return ColumnType.Float
                            }
                        }
                        return ColumnType.String
                    }

                    if (typeof (x) == 'number') {
                        if (Number.isInteger(x)) {
                            return ColumnType.Int
                        }
                        else {
                            return ColumnType.Float
                        }
                    }
                    return ColumnType.String
                }
                let ret = columns.map((x) => { return { name: x.name, dtype: x.values.length > 0 ? types(x.datatype, x.values[0]) : ColumnType.String } })

                // 初始化数据结构配置
                setSetting({ dimensions: { rows: [], columns: [] }, metrics: [], filters: [], rules: [], active: false, columns: new Map(ret.map(ret => [ret.name, ret.dtype])) })

                setColCfg(col)
                await main.setTitle(`数据表查询-${version} ${file.path}`);

            }
            else {
                setDataset({ ...dataset, csv: undefined, loading: false })
                await main.setTitle(`数据表查询-v${version}`);
            }
        }
    }

    // 保存CSV文本文件
    async function saveCsv() {
        let file = await utils.dialog.save({ title: "保存文件", defaultPath: await utils.path.appLocalDataDir(), filters: [{ name: "CSV 文本数据", extensions: ["csv"] }] })
        if (file) {
            let ret = await Services.actionSave(file);
            if (ret) {
                Notification.info({
                    closable: false,
                    title: '文件保存成功',
                    content: `结果文件已保存到${file}`,
                })
            }
        }
    }

    // 确认保存配置信息
    async function config() {
        console.info(setting);
        let ret = await Services.actionSetting(setting);
        if (ret) {

            console.info(ret);
            await onSearch(true);

            let filters = new Map<string, string[]>();
            await Promise.all(setting.filters.map(async x => {
                let values = await Services.column(x.index);
                filters.set(x.index, values);
            }))
            setColValues(filters);
            setSetting({ ...setting, active: true });
        }
        return ret;
    }

    // 查询数据
    async function onSearch(all = false) {
        setDataset({ ...dataset, reading: true });

        let ret = await Services.actionSearch(setting, all ? [] : search);

        setDataset({ ...dataset, reading: false, records: ret.records })

        let col = ret.columns.map((x: String) => {
            return {
                field: x,
                title: x,
                width: 'auto'
            }
        })

        setColCfg(col)
        if (all) {
            setSearch([]);
        }
    }
    // 分页查询更多数据
    async function onSearchMore(start: number) {
        let records = await Services.actionSearchMore(start);
        let ostart = dataset.records?.length;
        if (ostart == start && records.length > 0) {
            setDataset({ ...dataset, records: dataset.records?.concat(records) })
        }
    }

    // 当前配置状态 数据字段类型配置
    let onColumnTypeChange = (item: string, dtype: ColumnType) => {
        let columns = setting.columns;
        let newColumns = new Map<string, ColumnType>();
        columns.forEach((v, k) => {
            newColumns.set(k, k == item ? dtype : v)
        })
        setSetting({ ...setting, columns: newColumns })
    }
    // 当前配置状态 过滤条件字段
    let onFilterTypeChange = (item: Filter, mode: FilterMode) => {
        let filters = setting.filters;
        filters = filters.map(x => {
            return (x.index == item.index) ? { ...x, mode } : x
        })
        setSetting({ ...setting, filters })
    }

    // 当前配置状态 过滤条件字段类型
    let onSettingSearch = (value: string[]) => {
        let filters = value.map(x => {
            let o = setting.filters.find(f => f.index == x)
            return {
                index: x,
                mode: o ? o.mode : FilterMode.Multi
            }
        })
        setSetting({ ...setting, filters })
    }

    // 当前配置状态 数据维度行字段
    let onSettingRow = (value: string[]) => {
        let rows = value;
        setSetting({ ...setting, dimensions: { ...setting.dimensions, rows } })
    }
    // 当前配置状态 数据维度列字段
    let onSettingCol = (value: string[]) => {
        let columns = value;
        setSetting({ ...setting, dimensions: { ...setting.dimensions, columns } })
    }
    // 当前配置状态 数据指标字段
    let onSettingMetrics = (value: string[]) => {
        let metrics = value.map(x => {
            let o = setting.metrics.find(f => f.index == x)
            return {
                index: x,
                mode: o ? o.mode : MetricMode.Sum
            }
        })
        setSetting({ ...setting, metrics })
    }
    // 当前配置状态 数据指标字段统计类型
    let onMetricsModeChange = (item: Metric, mode: MetricMode) => {
        let metrics = setting.metrics;
        metrics = metrics.map(x => {
            return (x.index == item.index) ? { ...x, mode } : x
        })
        setSetting({ ...setting, metrics })
    }

    // 查询状态 查询条件配置
    let onFilterChange = (item: Filter, value: string[]) => {
        let exist = false;
        let ret = search.map(x => {
            if (x.index == item.index) {
                exist = true
                return { ...x, value }
            }
            return x
        })
        if (!exist) {
            ret.push({ ...item, value })
        }
        setSearch(ret)
    }

    // 查询组件
    const ui_search = () => {
        if (setting.filters.length == 0) {
            return <></>
        }
        return <div key={"tool_bar"} style={{ width: 260, display: "flex", flexDirection: "column" }}>
            <Space size={"medium"} direction={"vertical"} style={{ flex: 1 }}>
                {
                    setting.filters.map(x => {
                        switch (x.mode) {
                            case FilterMode.Single:
                                return <Select
                                    key={x.index}
                                    addBefore={x.index}
                                    value={search.find(v => v.index == x.index)?.value}
                                    placeholder=''
                                    tokenSeparators={[',', '|', '/']}
                                    allowClear={true}
                                    showSearch
                                    onChange={(value) => {
                                        onFilterChange(x, [value])
                                    }}
                                    onClear={() => {
                                        onFilterChange(x, [])
                                    }}
                                >
                                    {colValues.get(x.index)?.map((option, _index) => (
                                        <Select.Option key={option} value={option}>
                                            {option}
                                        </Select.Option>
                                    ))}
                                </Select>
                            case FilterMode.Multi:
                                return <Select
                                    key={x.index}
                                    addBefore={x.index}
                                    value={search.find(v => v.index == x.index)?.value}
                                    placeholder=''
                                    mode="multiple"
                                    tokenSeparators={[',', '|', '/']}
                                    allowClear={true}
                                    showSearch
                                    onChange={(value) => {
                                        onFilterChange(x, value)
                                    }}
                                    onClear={() => {
                                        onFilterChange(x, [])
                                    }}
                                >
                                    {colValues.get(x.index)?.map((option, _index) => (
                                        <Select.Option key={option} value={option}>
                                            {option}
                                        </Select.Option>
                                    ))}
                                </Select>

                            case FilterMode.MatchPrefix:
                                return <Input
                                    key={x.index}
                                    addBefore={x.index}
                                    value={search.find(v => v.index == x.index)?.value[0]}
                                    placeholder=''
                                    onChange={(value) => {
                                        onFilterChange(x, [value])
                                    }}
                                />

                            case FilterMode.DateRange:
                                return <>
                                    <DatePicker.RangePicker 
                                        placeholder={[`${x.index}开始`, `${x.index}结束`]}
                                        value={search.find(v => v.index == x.index)?.value}
                                        onChange={(value) => {
                                            onFilterChange(x, value)
                                        }}
                                        onClear={() => {
                                            onFilterChange(x, [])
                                        }} />
                                </>
                        }
                        return <></>
                    })
                }

            </Space>
            <Divider />
            <Layout.Footer>
                <Button type='primary' onClick={() => onSearch(false)}>
                    查询
                </Button>
                <Button type='default' onClick={() => onSearch(true)}>
                    清除
                </Button>
            </Layout.Footer>
        </div>
    }

    // 数据主体组件
    const ui = () => {
        if (dataset.loading) {
            return <>
                {`请稍后...`}
            </>
        }
        else if (dataset.csv) {
            return <div key={"data"} ref={refWrapper} style={{ display: "flex", flexDirection: "row", justifyContent: "center", flex: 1, padding: 16 }}>
                {ui_search()}
                <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
                    <ReactVTable.ListTable option={
                        {
                            theme: ReactVTable.VTable.themes.DARK,
                            widthMode: 'autoWidth',
                            records: dataset.records,
                            columns: colCfg,
                        }
                    } onScrollVerticalEnd={() => {
                        if (dataset.records) {
                            onSearchMore(dataset.records.length);
                        }
                    }}>
                    </ReactVTable.ListTable>
                </div>
                <Button style={{ position: "fixed", bottom: 48, right: 150 }} type='primary' onClick={() => setVisible(true)}>
                    <IconSettings />
                </Button>
                <Button style={{ position: "fixed", bottom: 48, right: 100 }} disabled={!setting.active} type='primary' onClick={() => saveCsv()}>
                    <IconShareExternal />
                </Button>
                <Button style={{ position: "fixed", bottom: 48, right: 48 }} type='primary' onClick={async () => {
                    setDataset({ ...DatasetDefault });
                    await main.setTitle(`数据表查询-${version}`);
                }}>
                    <IconClose />
                </Button>

                <Drawer
                    title='分析查询'
                    visible={visible}
                    width={"50%"}
                    getPopupContainer={() => refWrapper && refWrapper.current || document.body}
                    // footer={null}
                    onOk={() => {
                        config().then(x => {
                            if (x) {
                                setVisible(false);
                            }
                        })
                    }}
                    onCancel={() => {
                        setVisible(false);
                    }}
                >
                    {/* 配置 */}
                    <Space size='large' direction="vertical">
                        <Collapse
                            defaultActiveKey={['1']}
                            style={{ width: 600 }} >
                            <Collapse.Item header='字段类型配置' name='1'>
                                <List
                                    style={{ width: "100%", maxHeight: "80vh" }}
                                    dataSource={Array.from(setting.columns, ([key, value]) => { return { name: key, dtype: value } })}
                                    render={(item, index) => (
                                        <List.Item key={index}
                                            actionLayout='vertical'
                                            actions={[
                                                <div key={'s'} onClick={() => { onColumnTypeChange(item.name, ColumnType.String) }} style={{ padding: 8, backgroundColor: item.dtype == ColumnType.String ? '#551b94' : '#00000000' }}>
                                                    {'文本'}
                                                </div>,
                                                <div key={'i'} onClick={() => { onColumnTypeChange(item.name, ColumnType.Int) }} style={{ padding: 8, backgroundColor: item.dtype == ColumnType.Int ? '#551b94' : '#00000000' }}>
                                                    {'整数'}
                                                </div>,
                                                <div key={'f'} onClick={() => { onColumnTypeChange(item.name, ColumnType.Float) }} style={{ padding: 8, backgroundColor: item.dtype == ColumnType.Float ? '#551b94' : '#00000000' }}>
                                                    {'小数'}
                                                </div>,
                                                <div key={'d'} onClick={() => { onColumnTypeChange(item.name, ColumnType.Date) }} style={{ padding: 8, backgroundColor: item.dtype == ColumnType.Date ? '#551b94' : '#00000000' }}>
                                                    {'日期'}
                                                </div>,
                                            ]}
                                        >
                                            <List.Item.Meta
                                                title={`字段: ${item.name}`}
                                            />
                                        </List.Item>
                                    )}
                                />
                            </Collapse.Item>
                        </Collapse>
                        <Select
                            addBefore='查询列'
                            placeholder=''
                            defaultValue={setting.filters.map(x => x.index)}
                            style={{ width: 600 }}
                            mode="multiple"
                            tokenSeparators={[',', '|', '/']}
                            allowClear={true}
                            onChange={(value) => {
                                onSettingSearch(value)
                            }}
                        >
                            {dataset.columns?.map((option, _index) => (
                                <Select.Option key={option} value={option}>
                                    {option}
                                </Select.Option>
                            ))}
                        </Select>
                        <List
                            style={{ width: 600, maxHeight: "100%" }}
                            dataSource={setting.filters}
                            render={(item, index) => (
                                <List.Item key={index}
                                    actionLayout='vertical'
                                    actions={[
                                        <div key={'s'} onClick={() => { onFilterTypeChange(item, FilterMode.Single) }} style={{ padding: 8, backgroundColor: item.mode == FilterMode.Single ? '#551b94' : '#00000000' }}>
                                            {'单选'}
                                        </div>,
                                        <div key={'i'} onClick={() => { onFilterTypeChange(item, FilterMode.Multi) }} style={{ padding: 8, backgroundColor: item.mode == FilterMode.Multi ? '#551b94' : '#00000000' }}>
                                            {'多选'}
                                        </div>,
                                        <div key={'c'} onClick={() => { onFilterTypeChange(item, FilterMode.MatchPrefix) }} style={{ padding: 8, backgroundColor: item.mode == FilterMode.MatchPrefix ? '#551b94' : '#00000000' }}>
                                            {'匹配'}
                                        </div>,
                                        <div key={'d'} onClick={() => { onFilterTypeChange(item, FilterMode.DateRange) }} style={{ padding: 8, backgroundColor: item.mode == FilterMode.DateRange ? '#551b94' : '#00000000' }}>
                                            {'日期'}
                                        </div>,
                                    ]}
                                >
                                    <List.Item.Meta
                                        title={`查询项: ${item.index}`}
                                    />
                                </List.Item>
                            )}
                        />
                        <Select
                            addBefore='行维度'
                            placeholder=''
                            defaultValue={setting.dimensions.rows}
                            style={{ width: 600 }}
                            mode="multiple"
                            tokenSeparators={[',', '|', '/']}
                            allowClear={true}
                            onChange={(value) =>
                                onSettingRow(value)
                            }
                        >
                            {Array.from(setting.columns, ([key, value]) => { return { name: key, dtype: value } }).filter(x => x.dtype == ColumnType.String || x.dtype == ColumnType.Date).map((option, _index) => (
                                <Select.Option key={option.name} value={option.name}>
                                    {option.name}
                                </Select.Option>
                            ))}
                        </Select>
                        <Select addBefore='列维度'
                            style={{ width: 600 }}
                            defaultValue={setting.dimensions.columns}
                            mode='multiple'
                            tokenSeparators={[',', '|', '/']}
                            allowClear={true}
                            onChange={(value) => {
                                onSettingCol(value)
                            }}
                        >
                            {Array.from(setting.columns, ([key, value]) => { return { name: key, dtype: value } }).filter(x => x.dtype == ColumnType.String || x.dtype == ColumnType.Date).map((option, _index) => (
                                <Select.Option key={option.name} value={option.name}>
                                    {option.name}
                                </Select.Option>
                            ))}
                        </Select>
                        <Select
                            addBefore='指标'
                            placeholder=''
                            defaultValue={setting.metrics.map(x => x.index)}
                            style={{ width: 600 }}
                            mode="multiple"
                            tokenSeparators={[',', '|', '/']}
                            allowClear={true}
                            onChange={(value) => {
                                onSettingMetrics(value)
                            }}
                        >
                            {dataset.columns?.map((option, _index) => (
                                <Select.Option key={option} value={option}>
                                    {option}
                                </Select.Option>
                            ))}
                        </Select>
                        <List
                            style={{ width: 600, maxHeight: "100%" }}
                            dataSource={setting.metrics}
                            render={(item, index) => (
                                <List.Item key={index}
                                    actionLayout='vertical'
                                    actions={[
                                        <div key={'sum'} onClick={() => { onMetricsModeChange(item, MetricMode.Sum) }} style={{ padding: 8, backgroundColor: item.mode == MetricMode.Sum ? '#551b94' : '#00000000' }}>
                                            {'合计'}
                                        </div>,
                                        <div key={'count'} onClick={() => { onMetricsModeChange(item, MetricMode.Count) }} style={{ padding: 8, backgroundColor: item.mode == MetricMode.Count ? '#551b94' : '#00000000' }}>
                                            {'计数'}
                                        </div>,
                                        <div key={'avg'} onClick={() => { onMetricsModeChange(item, MetricMode.Avg) }} style={{ padding: 8, backgroundColor: item.mode == MetricMode.Avg ? '#551b94' : '#00000000' }}>
                                            {'均差'}
                                        </div>,
                                        <div key={'rate'} onClick={() => { onMetricsModeChange(item, MetricMode.Rate) }} style={{ padding: 8, backgroundColor: item.mode == MetricMode.Rate ? '#551b94' : '#00000000' }}>
                                            {'占比'}
                                        </div>,
                                    ]}
                                >
                                    <List.Item.Meta
                                        title={`查询项: ${item.index}`}
                                    />
                                </List.Item>
                            )}
                        />
                    </Space>
                </Drawer>
            </div>
        }
        else {
            return <div style={{ display: "flex", flexDirection: "row", justifyContent: "center" }}>
                <div style={{ minWidth: 120, padding: 10 }} className='arco-upload-trigger-picture' onClick={openCsv}>
                    <div className='arco-upload-trigger-picture-text'>
                        <IconPlus />
                        <div style={{ marginTop: 10, fontWeight: 600 }}>请选择数据文件</div>
                    </div>
                </div>
            </div>
        }
    }

    return (
        <Spin loading={dataset.reading}>
            <div className="container">
                {ui()}
            </div>
        </Spin>
    );
}

export default App;
