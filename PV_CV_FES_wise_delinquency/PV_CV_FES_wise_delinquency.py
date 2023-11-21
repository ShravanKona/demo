

import pandas as pd
from custom_functions.connection import sandbox
from custom_functions.gp_tb_details import remove_lr,remove_cancelled,flatten
from glob import glob
from tqdm import tqdm
tqdm.pandas()


df_stat = pd.read_sql('''SELECT [NEW_HPANO]
      ,ACCT_YYYYMM,FIN_AMOUNT,BUS_VERTICAL
  FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] 
  where ACCT_YYYYMM between '201904' and '202303'
  ''',sandbox())
  
  
df_stat = remove_lr(df_stat)
df_stat = remove_cancelled(df_stat)

df_stat['FY'] = pd.to_datetime(df_stat['ACCT_YYYYMM'],format='%Y%m').dt.to_period('Q-Mar').dt.qyear



df_stat2 = pd.read_sql('''SELECT A.[NEW_HPANO]
      ,[HPA_DATE]
      ,[MIS_SECTOR],MIS_PRODUCT,MIS_SUB_PRODUCT,MIS_GROUP_MM
      ,[STATE_DES]
      ,[BUS_VERTICAL]
	  ,BRANCH
  FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] as A,[MMFSL_SandBox].[dbo].[TB_EXTN_DTLS] as B
  where A.NEW_HPANO = B.NEW_HPANO
  ''',sandbox())

df_stat2['NEW_HPANO'] = df_stat2['NEW_HPANO'].astype(str)
df_stat2['NEW_HPANO'] = df_stat2['NEW_HPANO'].str.lstrip('0')


df_pv = df_stat2[df_stat2['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_pv = df_pv[((df_pv.MIS_PRODUCT.values=='PER SEGMENT')&(df_pv.MIS_SECTOR.values=='AS'))
              |((df_pv.MIS_SECTOR.values=='LMV')&(df_pv.MIS_GROUP_MM.values!='MARUTI SUPER CARRY'))]



df_fes = df_stat2[df_stat2['BUS_VERTICAL'] == 'BFE'].copy()
tractor = ['TRACTOR ',  'TRACTOR WITH TRAILOR','TRACTOR WITH TRACTOR MOUTED COMBINE HARVESTOR',
       'TRACTOR WITH LOADER', 'TRACTOR WITH BULL LOADER','TRACTOR WITH AIR COMPRESSOR', 'TRACTOR WITH DOZER',
       'TRACTOR WITH LEVELLER', 'TRACTOR WITH BACKHOE AND LOADER','TRACTOR WITH LOADER & DOZER',
       'TRACTOR WITH TRAILOR AND LOADER', 'TRACTOR']
df_fes = df_fes[df_fes['MIS_SUB_PRODUCT'].isin(tractor)]

df_cv_ce = df_stat2[df_stat2['BUS_VERTICAL'].isin(['BCV'])].copy()
df_cv_ce = df_cv_ce[df_cv_ce['MIS_SECTOR'].isin(['CV-CE'])]

df_scv = df_stat2[df_stat2['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_scv = df_scv[(df_scv['MIS_GROUP_MM'].isin(['MARUTI SUPER CARRY','SUPRO MAXI TRUCK', 
                                              'SUPRO VAN', 'SUPRO MINI TRUCK', 'SUPRO MINI VAN',
                                              'JEETO LOAD', 'JEETO PASSENGER','MAXI TRUCK','PICK UP']))]




df_pv['TAG'] = 'BAU & BLM (PV)'
df_scv['TAG'] = 'CV-CE'
df_cv_ce['TAG'] = 'CV-CE'
df_fes['TAG'] = 'FES'

df_tagged = pd.concat([df_pv,df_scv,df_cv_ce,df_fes])


df_stat = pd.merge(df_stat,df_tagged[['NEW_HPANO','TAG']],on='NEW_HPANO',how='inner') 


gpy = df_stat.groupby(['TAG','FY'],as_index=False)['FIN_AMOUNT'].sum()



from glob import glob
from tqdm import tqdm
cols = ['NEW_HPANO','acct_yyyymm',
  'Apr_AGE','May_AGE','Jun_AGE','Jul_AGE','Aug_AGE','Sep_AGE','Oct_AGE','Nov_AGE','Dec_AGE','Jan_AGE','Feb_AGE','Mar_AGE',
  'Apr_STATUS','May_STATUS','Jun_STATUS','Jul_STATUS','Aug_STATUS','Sep_STATUS','Oct_STATUS','Nov_STATUS','Dec_STATUS','Jan_STATUS','Feb_STATUS','Mar_STATUS',
  'Apr_cd','May_cd','Jun_cd','Jul_cd','Aug_cd','Sep_cd','Oct_cd','Nov_cd','Dec_cd','Jan_cd','Feb_cd','Mar_cd',
  'Apr_SOH','May_SOH','Jun_SOH','Jul_SOH','Aug_SOH','Sep_SOH','Oct_SOH','Nov_SOH','Dec_SOH','Jan_SOH','Feb_SOH','Mar_SOH',
  'Apr_OUTSTAND','May_OUTSTAND','Jun_OUTSTAND','Jul_OUTSTAND','Aug_OUTSTAND','Sep_OUTSTAND','Oct_OUTSTAND','Nov_OUTSTAND','Dec_OUTSTAND','Jan_OUTSTAND','Feb_OUTSTAND','Mar_OUTSTAND',
  'FY_Period']

df_gp = pd.concat([pd.read_parquet(i,columns = cols) for i in tqdm([r'A:/Sandbox/GP_TB_Details/FY2024.parquet',
                                                                    r'A:/Sandbox/GP_TB_Details/FY2023.parquet',
                                                                    r'A:/Sandbox/GP_TB_Details/FY2022.parquet'])])


df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].astype(str)
df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].str.lstrip('0')


df_gp = flatten(df_gp)

df_gp = df_gp[(df_gp['STATUS'].isin(['L','R','U','6','7','8'])) & 
              (df_gp[['SOH','OUTSTAND']].sum(axis=1)>0)]

df_gp['YYYYMM'] = df_gp['YYYYMM'].astype(str)
yyyymm = ['201912','202003','202203','202303']

df_gp = df_gp[df_gp['YYYYMM'].isin(yyyymm)]


df_aum = pd.read_sql(''' 
                 SELECT [YYYYMM]
                  ,[HPA_NO] as NEW_HPANO
                  ,[TAG]
                  ,[CLS_AMORTIZED_COST] + [CLS_OD] as AUM
              FROM [MMFSL_SandBox].[dbo].[TBL_GNPA_HISTORY_TABLEAU]
              where YYYYMM in ({yyyymm})
                 '''.format(yyyymm = "'" + "','".join(yyyymm) +"'" ),sandbox())
                 
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].astype(str)
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].str.lstrip('0')
df_aum['YYYYMM'] = df_aum['YYYYMM'].astype(str)



df_gp = pd.merge(df_gp,df_aum,on=['NEW_HPANO','YYYYMM'],how='left')

df_gp['30-179'] = (df_gp['AGE'].between(2,6))*df_gp['AUM']
df_gp['90-179'] = (df_gp['AGE'].between(4,6))*df_gp['AUM']
df_gp['180-359'] = (df_gp['AGE'].between(7,12))*df_gp['AUM']
df_gp['360-719'] = (df_gp['AGE'].between(13,24))*df_gp['AUM']
df_gp['720-1079'] = (df_gp['AGE'].between(25,36))*df_gp['AUM']
df_gp['90+'] = (df_gp['AGE']>3)*df_gp['AUM']



df_gp['30-359'] = (df_gp['AGE'].between(2,12))*df_gp['AUM']
df_gp['90-359'] = (df_gp['AGE'].between(4,12))*df_gp['AUM']
df_gp['30-719'] = (df_gp['AGE'].between(2,24))*df_gp['AUM']
df_gp['90-719'] = (df_gp['AGE'].between(4,24))*df_gp['AUM']


df_stat = pd.read_sql('''SELECT A.[NEW_HPANO]
      ,[HPA_DATE]
      ,[MIS_SECTOR],MIS_PRODUCT,MIS_SUB_PRODUCT,MIS_GROUP_MM
      ,[STATE_DES]
      ,[BUS_VERTICAL]
	  ,BRANCH
  FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] as A,[MMFSL_SandBox].[dbo].[TB_EXTN_DTLS] as B
  where A.NEW_HPANO = B.NEW_HPANO
  ''',sandbox())

df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].astype(str)
df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].str.lstrip('0')


df_pv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_pv = df_pv[((df_pv.MIS_PRODUCT.values=='PER SEGMENT')&(df_pv.MIS_SECTOR.values=='AS'))
              |((df_pv.MIS_SECTOR.values=='LMV')&(df_pv.MIS_GROUP_MM.values!='MARUTI SUPER CARRY'))]



df_fes = df_stat[df_stat['BUS_VERTICAL'] == 'BFE'].copy()
tractor = ['TRACTOR ',  'TRACTOR WITH TRAILOR','TRACTOR WITH TRACTOR MOUTED COMBINE HARVESTOR',
       'TRACTOR WITH LOADER', 'TRACTOR WITH BULL LOADER','TRACTOR WITH AIR COMPRESSOR', 'TRACTOR WITH DOZER',
       'TRACTOR WITH LEVELLER', 'TRACTOR WITH BACKHOE AND LOADER','TRACTOR WITH LOADER & DOZER',
       'TRACTOR WITH TRAILOR AND LOADER', 'TRACTOR']
df_fes = df_fes[df_fes['MIS_SUB_PRODUCT'].isin(tractor)]

df_cv_ce = df_stat[df_stat['BUS_VERTICAL'].isin(['BCV'])].copy()
df_cv_ce = df_cv_ce[df_cv_ce['MIS_SECTOR'].isin(['CV-CE'])]

df_scv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_scv = df_scv[(df_scv['MIS_GROUP_MM'].isin(['MARUTI SUPER CARRY','SUPRO MAXI TRUCK', 
                                              'SUPRO VAN', 'SUPRO MINI TRUCK', 'SUPRO MINI VAN',
                                              'JEETO LOAD', 'JEETO PASSENGER','MAXI TRUCK','PICK UP']))]




df_pv['TAG'] = 'BAU & BLM (PV)'
df_scv['TAG'] = 'CV-CE'
df_cv_ce['TAG'] = 'CV-CE'
df_fes['TAG'] = 'FES'

df_tagged = pd.concat([df_pv,df_scv,df_cv_ce,df_fes])


df_gp_w_tag = pd.merge(df_gp,df_tagged[['NEW_HPANO','TAG']],on='NEW_HPANO',how='inner') 


gpy = df_gp_w_tag.groupby(['YYYYMM','TAG_y'])[['AUM', 
                                               '30-179', '90-179', 
                                               '30-359', 
                                               '90-359', 
                                               '30-719',
                                               '90-719']].sum()


gpy = df_gp_w_tag.groupby(['YYYYMM','TAG_y'])[['AUM', 
                                               '90-179',
                                               '180-359', 
                                               '360-719', 
                                               '720-1079', 
                                               '90+']].sum()



##################################################################################################################################


import pandas as pd
from custom_functions.connection import sandbox
from custom_functions.gp_tb_details import remove_lr,remove_cancelled,flatten
from glob import glob
from tqdm import tqdm
tqdm.pandas()


gp_list = glob(r'Z:\GP_TB_DETAILS\*.pkl')
gp_list = gp_list[9:]

df_gp = pd.concat([pd.read_pickle(i) for i in tqdm(gp_list)])

cols = ['NEW_HPANO',  'Apr_SOH', 'May_SOH', 'Jun_SOH', 'Jul_SOH', 'Aug_SOH', 'Sep_SOH', 'Oct_SOH',
 'Nov_SOH', 'Dec_SOH', 'Jan_SOH', 'Feb_SOH', 'Mar_SOH', 'Apr_OUTSTAND', 'May_OUTSTAND', 'Jun_OUTSTAND',
 'Jul_OUTSTAND', 'Aug_OUTSTAND', 'Sep_OUTSTAND', 'Oct_OUTSTAND', 'Nov_OUTSTAND', 'Dec_OUTSTAND', 'Jan_OUTSTAND', 'Feb_OUTSTAND',
 'Mar_OUTSTAND','Apr_STATUS', 'May_STATUS', 'Jun_STATUS', 'Jul_STATUS', 'Aug_STATUS', 'Sep_STATUS', 'Oct_STATUS',
 'Nov_STATUS', 'Dec_STATUS', 'Jan_STATUS', 'Feb_STATUS', 'Mar_STATUS',
 'Apr_AGE', 'May_AGE', 'Jun_AGE', 'Jul_AGE', 'Aug_AGE', 'Sep_AGE', 'Oct_AGE',
 'Nov_AGE', 'Dec_AGE', 'Jan_AGE', 'Feb_AGE', 'Mar_AGE'
 ,'FY_Period']

df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].astype(str)
df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].str.lstrip('0')


df_gp = flatten(df_gp[cols])

df_gp = df_gp[(df_gp['STATUS'].isin(['L','R','U','6','7','8'])) & 
              (df_gp[['SOH','OUTSTAND']].sum(axis=1)>0)]

df_gp['YYYYMM'] = df_gp['YYYYMM'].astype(str)
yyyymm = ['201912','202003','202006','202012','202103','202106','202112','202203'
          ,'202206','202212','202303']

df_gp = df_gp[df_gp['YYYYMM'].isin(yyyymm)]


df_aum = pd.read_sql(''' 
                 SELECT [YYYYMM]
                  ,[HPA_NO] as NEW_HPANO
                  ,[TAG]
                  ,[CLS_AMORTIZED_COST] + [CLS_OD] as AUM
              FROM [MMFSL_SandBox].[dbo].[TBL_GNPA_HISTORY_TABLEAU]
              where YYYYMM in ({yyyymm})
                 '''.format(yyyymm = "'" + "','".join(yyyymm) +"'" ),sandbox())
                 
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].astype(str)
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].str.lstrip('0')
df_aum['YYYYMM'] = df_aum['YYYYMM'].astype(str)



df_gp = pd.merge(df_gp,df_aum,on=['NEW_HPANO','YYYYMM'],how='left')



df_stat = pd.read_sql('''SELECT A.[NEW_HPANO]
      ,[HPA_DATE]
      ,[MIS_SECTOR],MIS_PRODUCT,MIS_SUB_PRODUCT,MIS_GROUP_MM
      ,[STATE_DES]
      ,[BUS_VERTICAL]
      ,TENURE
      ,FIN_AMOUNT
      ,INV_VALUE
	  ,BRANCH
  FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] as A,[MMFSL_SandBox].[dbo].[TB_EXTN_DTLS] as B
  where A.NEW_HPANO = B.NEW_HPANO
  ''',sandbox())

df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].astype(str)
df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].str.lstrip('0')



df_pv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU','BRE'])].copy()
df_pv = df_pv[((df_pv.MIS_PRODUCT.values=='PER SEGMENT')&(df_pv.MIS_SECTOR.values=='AS'))
              |((df_pv.MIS_SECTOR.values=='LMV')&(df_pv.MIS_GROUP_MM.values!='MARUTI SUPER CARRY'))]



df_fes = df_stat[df_stat['BUS_VERTICAL'] == 'BFE'].copy()
tractor = ['TRACTOR ',  'TRACTOR WITH TRAILOR','TRACTOR WITH TRACTOR MOUTED COMBINE HARVESTOR',
       'TRACTOR WITH LOADER', 'TRACTOR WITH BULL LOADER','TRACTOR WITH AIR COMPRESSOR', 'TRACTOR WITH DOZER',
       'TRACTOR WITH LEVELLER', 'TRACTOR WITH BACKHOE AND LOADER','TRACTOR WITH LOADER & DOZER',
       'TRACTOR WITH TRAILOR AND LOADER', 'TRACTOR']
df_fes = df_fes[df_fes['MIS_SUB_PRODUCT'].isin(tractor)]

df_cv_ce = df_stat[df_stat['BUS_VERTICAL'].isin(['BCV'])].copy()
df_cv_ce = df_cv_ce[df_cv_ce['MIS_SECTOR'].isin(['CV-CE'])]

df_scv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_scv = df_scv[(df_scv['MIS_GROUP_MM'].isin(['MARUTI SUPER CARRY','SUPRO MAXI TRUCK', 
                                              'SUPRO VAN', 'SUPRO MINI TRUCK', 'SUPRO MINI VAN',
                                              'JEETO LOAD', 'JEETO PASSENGER','MAXI TRUCK','PICK UP']))]




df_pv['TAG'] = 'BAU,BLM & BRE (PV)'
df_scv['TAG'] = 'CV-CE'
df_cv_ce['TAG'] = 'CV-CE'
df_fes['TAG'] = 'FES'

df_tagged = pd.concat([df_pv,df_scv,df_cv_ce,df_fes])

df_mapping = pd.read_excel(r'Z:/SpotVM_Output/Downturn/CIRCLE_MAPPING.xlsx')

df_stat = pd.merge(df_stat,df_mapping,on='BRANCH',how='left')

df_gp.isna().sum()

df_stat['LTV'] = df_stat['FIN_AMOUNT']/df_stat['INV_VALUE']

df_gp = pd.merge(df_gp,df_stat,on='NEW_HPANO',how='left')
df_gp = pd.merge(df_gp,df_tagged,on='NEW_HPANO',how='inner')

gpy = df_gp.groupby(['YYYYMM','TAG'],as_index=False)[['LTV_y','TENURE_y']].mean()

gpy['YYYYMM'] = pd.to_datetime(gpy['YYYYMM'],format='%Y%m').dt.strftime("%b-%y")


gpy2 = df_gp.groupby(['YYYYMM','CIRCLE_NAME','STATE_DES'],as_index=False)[['AUM']].sum()
gpy2['YYYYMM'] = pd.to_datetime(gpy2['YYYYMM'],format='%Y%m').dt.strftime("%b-%y")



#############################################################################################




import pandas as pd
from custom_functions.connection import sandbox
from custom_functions.gp_tb_details import remove_lr,remove_cancelled,flatten
from glob import glob
from tqdm import tqdm
tqdm.pandas()


gp_list = glob(r'Z:\GP_TB_DETAILS\*.pkl')
gp_list = gp_list[9:]

df_gp = pd.concat([pd.read_pickle(i) for i in tqdm(gp_list)])

cols = ['NEW_HPANO',  'Apr_SOH', 'May_SOH', 'Jun_SOH', 'Jul_SOH', 'Aug_SOH', 'Sep_SOH', 'Oct_SOH',
 'Nov_SOH', 'Dec_SOH', 'Jan_SOH', 'Feb_SOH', 'Mar_SOH', 'Apr_OUTSTAND', 'May_OUTSTAND', 'Jun_OUTSTAND',
 'Jul_OUTSTAND', 'Aug_OUTSTAND', 'Sep_OUTSTAND', 'Oct_OUTSTAND', 'Nov_OUTSTAND', 'Dec_OUTSTAND', 'Jan_OUTSTAND', 'Feb_OUTSTAND',
 'Mar_OUTSTAND','Apr_STATUS', 'May_STATUS', 'Jun_STATUS', 'Jul_STATUS', 'Aug_STATUS', 'Sep_STATUS', 'Oct_STATUS',
 'Nov_STATUS', 'Dec_STATUS', 'Jan_STATUS', 'Feb_STATUS', 'Mar_STATUS',
 'Apr_AGE', 'May_AGE', 'Jun_AGE', 'Jul_AGE', 'Aug_AGE', 'Sep_AGE', 'Oct_AGE',
 'Nov_AGE', 'Dec_AGE', 'Jan_AGE', 'Feb_AGE', 'Mar_AGE'
 ,'FY_Period']

df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].astype(str)
df_gp['NEW_HPANO'] = df_gp['NEW_HPANO'].str.lstrip('0')


df_gp = flatten(df_gp[cols])

df_gp = df_gp[(df_gp['STATUS'].isin(['L','R','U','6','7','8'])) & 
              (df_gp[['SOH','OUTSTAND']].sum(axis=1)>0)]

df_gp['YYYYMM'] = df_gp['YYYYMM'].astype(str)
yyyymm = ['202106','202203','202303','202302','202202']

df_gp = df_gp[df_gp['YYYYMM'].isin(yyyymm)]

df_aum = pd.read_sql(''' 
                 SELECT [YYYYMM]
                  ,[HPA_NO] as NEW_HPANO
                  ,[TAG]
                  ,[CLS_AMORTIZED_COST] + [CLS_OD] as AUM
              FROM [MMFSL_SandBox].[dbo].[TBL_GNPA_HISTORY_TABLEAU]
              where YYYYMM in ({yyyymm})
                 '''.format(yyyymm = "'" + "','".join(yyyymm) +"'" ),sandbox())
                 
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].astype(str)
df_aum['NEW_HPANO'] = df_aum['NEW_HPANO'].str.lstrip('0')
df_aum['YYYYMM'] = df_aum['YYYYMM'].astype(str)



df_gp = pd.merge(df_gp,df_aum,on=['NEW_HPANO','YYYYMM'],how='left')



df_stat = pd.read_sql('''SELECT A.[NEW_HPANO]
      ,[HPA_DATE]
      ,[MIS_SECTOR],MIS_PRODUCT,MIS_SUB_PRODUCT,MIS_GROUP_MM
      ,[STATE_DES]
      ,[BUS_VERTICAL]
      ,TENURE
      ,FIN_AMOUNT
      ,INV_VALUE
	  ,BRANCH
  FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] as A,[MMFSL_SandBox].[dbo].[TB_EXTN_DTLS] as B
  where A.NEW_HPANO = B.NEW_HPANO
  ''',sandbox())

df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].astype(str)
df_stat['NEW_HPANO'] = df_stat['NEW_HPANO'].str.lstrip('0')



df_pv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_pv = df_pv[((df_pv.MIS_PRODUCT.values=='PER SEGMENT')&(df_pv.MIS_SECTOR.values=='AS'))
              |((df_pv.MIS_SECTOR.values=='LMV')&(df_pv.MIS_GROUP_MM.values!='MARUTI SUPER CARRY'))]



df_fes = df_stat[df_stat['BUS_VERTICAL'] == 'BFE'].copy()
tractor = ['TRACTOR ',  'TRACTOR WITH TRAILOR','TRACTOR WITH TRACTOR MOUTED COMBINE HARVESTOR',
       'TRACTOR WITH LOADER', 'TRACTOR WITH BULL LOADER','TRACTOR WITH AIR COMPRESSOR', 'TRACTOR WITH DOZER',
       'TRACTOR WITH LEVELLER', 'TRACTOR WITH BACKHOE AND LOADER','TRACTOR WITH LOADER & DOZER',
       'TRACTOR WITH TRAILOR AND LOADER', 'TRACTOR']
df_fes = df_fes[df_fes['MIS_SUB_PRODUCT'].isin(tractor)]

df_cv_ce = df_stat[df_stat['BUS_VERTICAL'].isin(['BCV'])].copy()
df_cv_ce = df_cv_ce[df_cv_ce['MIS_SECTOR'].isin(['CV-CE'])]

df_scv = df_stat[df_stat['BUS_VERTICAL'].isin(['BLM','BAU'])].copy()
df_scv = df_scv[(df_scv['MIS_GROUP_MM'].isin(['MARUTI SUPER CARRY','SUPRO MAXI TRUCK', 
                                              'SUPRO VAN', 'SUPRO MINI TRUCK', 'SUPRO MINI VAN',
                                              'JEETO LOAD', 'JEETO PASSENGER','MAXI TRUCK','PICK UP']))]




df_pv['TAG'] = 'BAU & BLM (PV)'
df_scv['TAG'] = 'CV-CE'
df_cv_ce['TAG'] = 'CV-CE'
df_fes['TAG'] = 'FES'

df_tagged = pd.concat([df_pv,df_scv,df_cv_ce,df_fes])

df_gp2 = pd.merge(df_gp,df_tagged[['NEW_HPANO','TAG']],on='NEW_HPANO',how='inner')

df_gp2['30+ by AUM'] = (df_gp2['AGE']>=2)*df_gp2['AUM']
df_gp2['90+ by AUM'] = (df_gp2['AGE']>=4)*df_gp2['AUM']

gpy = df_gp2.groupby(['YYYYMM','TAG_y'],as_index=False)[['30+ by AUM','90+ by AUM','AUM']].sum()
gpy['YYYYMM'] = pd.to_datetime(gpy['YYYYMM'],format='%Y%m').dt.strftime("%b-%y")







