import { getVersion } from '@tauri-apps/api/app';

import * as core from "@tauri-apps/api/core";
import * as path from "@tauri-apps/api/path";
import * as dialog from "@tauri-apps/plugin-dialog";
import * as clipboard from '@tauri-apps/plugin-clipboard-manager';

import { ISetting, SearchItem, ServiceColumnType } from './types';

const Services = {
    load: async (file: string) => {
        let ret = await core.invoke("databoard_loader", { path: file });
        return ret;
    },
    count: async () => {
        return await core.invoke("databoard_count", {});
    },
    columns: async (): Promise<ServiceColumnType[]> => {
        let ret: any = await core.invoke("databoard_columns", {})
        return ret.columns;
    },
    preview: async (): Promise<any[]> => {
        return await core.invoke("databoard_preview", { count: 100 });
    },
    column: async (name: string): Promise<string[]> => {
        let ret: any = await core.invoke("databoard_unique", { name })
        let values: string[] = [];
        switch (ret.datatype) {
            case "Date":
                let pad = (x: number) => x < 10 ? `0${x}` : `${x}`;
                values = ret.values.map((x: number) => x * 86400000).map((x: number) => {
                    let date = new Date(x);
                    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
                })
                break;
            default:
                values = ret.values.map((x: any) => String(x))
                break;
        }
        return values;
    },

    actionSetting: async (setting: ISetting): Promise<boolean> => {
        return await core.invoke("databoard_setting", { setting: { columns: setting.columns } });
    },
    actionSearch: async (setting: ISetting, search: SearchItem[]): Promise<any> => {
        return await core.invoke("databoard_search", { playload: { ...setting, search } });
    },
    actionSearchMore: async (start: number): Promise<any[]> => {
        return await core.invoke("databoard_search_more", { start });
    },
    actionSave: async (file: string) => {
        return await core.invoke("databoard_search_save", { path: file });
    }
}

export {
    getVersion, Services, path, dialog, clipboard
};
