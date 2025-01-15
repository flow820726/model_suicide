import pandas as pd
import numpy as np
from scipy import stats

def create_table_one(df, group, col, cate_col):
    g_v = list(df[group].unique())
    tb_1 = pd.DataFrame()
    for i in col:
        if i not in cate_col:
            # na
            df_temp = df[[group, i]].copy()
            na_count = df_temp[df_temp[i] == -999].shape[0]
            all_count = df_temp.shape[0]
            na_r = np.round(na_count / all_count,2)
            df_temp[i] = np.where(df_temp[i] == -999, np.nan, df_temp[i])

            # mean
            overall_mean = np.round(df_temp[i].mean(), 2)
            overall_sd = np.round(df_temp[i].std(), 2)

            # median
            overall_median = np.round(df_temp[i].dropna().median(), 2)
            overall_iqr = np.round(df_temp[i].dropna().quantile(.75) - df_temp[i].dropna().quantile(.25), 2)

            # group test
            df_test = df_temp.dropna()
            gb = df_test.groupby(group)[i]
            gb = [gb.get_group(x) for x in gb.groups]

            s, t_p = stats.ttest_ind(gb[0], gb[1])
            w, w_p = stats.mannwhitneyu(gb[0], gb[1])

            # group stat
            df_c = df_temp.groupby(group)[i].agg(
                [('mean(sd)',
                  lambda value: str(np.round(np.mean(value), 2)) + "(" + str(np.round(np.std(value), 2)) + ")"),
                 ('median(iqr)', lambda value: str(np.round(np.nanmedian(value), 2)) + "(" +
                                               str(np.round(np.nanquantile(value, q=.75) - np.nanquantile(value, q=.25),
                                                            2)) + ")"),
                 ('na', lambda value: str(sum(np.isnan(value))) + "(" +  str(np.round(sum(np.isnan(value)) / len(value),2)) + ")" )
                ]).reset_index()
            df_c = df_c.melt(id_vars=group, var_name="stat")
            df_c = df_c.pivot(index='stat', columns=group, values='value').reset_index()
            df_c['p_value'] = [t_p, w_p, np.nan]
            df_c['p_value'] = np.where(df_c['p_value'] < 0.001, '< 0.001', np.round(df_c['p_value'],3))

            df_c['overall'] = [str(overall_mean) + '(' + str(overall_sd) + ")",
                               str(overall_median) + '(' + str(overall_iqr) + ")",
                               str(na_count) + '(' + str(na_r) + ")"]

        else:

            # calculate info
            df_c1 = pd.crosstab(df[i], df[group],
                                margins=True, margins_name="Total", dropna = False).reset_index() # drop na = Fasle new
            df_c1 = df_c1.rename(columns={i: "stat",
                                          "Total": "overall"})
            df_c1 = df_c1.melt(id_vars="stat", var_name="var")
            df_c1_t = df_c1[df_c1['stat'] == "Total"][['var', 'value']]
            df_c1_t = df_c1_t.rename(columns={"value": "t"})

            df_c1 = df_c1.merge(df_c1_t, on=['var'])
            df_c1['p'] = (np.round(df_c1['value'] / df_c1['t'], 4) * 100).astype(str).str[:5]
            df_c1['v'] = df_c1['value'].astype(str) + "(" + df_c1['p'].astype(str) + "%)"

            df_c1 = df_c1[df_c1['stat'] != "Total"].copy()
            df_c = df_c1.pivot(index='stat', columns='var', values='v').reset_index()

            # test
            df_t = df_c1.pivot(index='stat', columns='var', values='value').reset_index()
            
            # add 1 new
            df_t[g_v] +=1
            
            g, p, dof, expctd = stats.chi2_contingency(df_t[g_v].to_numpy())
            df_c['p_value'] = p
            df_c['p_value'] = np.where(df_c['p_value'] < 0.001, '< 0.001', np.round(df_c['p_value'],3))

        df_c['variable'] = i
        tb_1 = pd.concat([tb_1, df_c])
        print(i + " Done==")

    df['n'] = 1
    df_n = df.groupby(group)['n'].count().reset_index()
    tb_1 = tb_1[[tb_1.columns[-1]] + list(tb_1.columns[:-1])]
    tb_1 = tb_1.rename(columns={g_v[0]: str(g_v[0]) + '(n:' + str(df_n[df_n[group] == g_v[0]]['n'].values[0]) + ')',
                                g_v[1]: str(g_v[1]) + '(n:' + str(df_n[df_n[group] == g_v[1]]['n'].values[0]) + ')',
                                'overall': 'overall' + "(n:" + str(df.shape[0]) + ")"})

    return tb_1
