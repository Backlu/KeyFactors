#!/usr/bin/env python
# coding: utf-8

# History:
# 8/13
#  - 對應MBSN的時候考慮維修的狀況
#  - 組part的時候考慮維修的狀況
# 8/14
#  - parse vendorcode, DC
#  -
# 8/17
#  - 撿料: 同一筆測試可能有兩筆{Part}維修紀錄, 保留測試後的第一筆
#  - 過站: 同一筆測試可能有兩筆{station}過站紀錄, 保留測試前的最後一筆
# 8/18
#  - remove missing values > 30% feature
#  - remove single unique feature
# 8/19 
#  - class
#  - 修改檔名(英文小寫)
#  - 刪除不需要的sheet

# TODO:
#1. 需要請IT統一提供資料的時間格式, 不能放在程式裡處理
#2. 需要請IT統一提供資料的欄位, 檔案命名格式, 檔案格式從xlsx改為csv
#3. 時間區段的集中性分析
#4. data路徑以後要改成存放data的絕對路徑


import os
from datetime import datetime
import numpy as np
import pandas as pd
import scipy as sc
from keyfactordiag.feature_selector import FeatureSelector


class Diag(object):
    _defaults = {
        'dpath_fatptest' : 'sampledata/1_fatp_test.xlsx',
        'dpath_fatprepair' : 'sampledata/2_fatp_repair.xlsx',
        'dpath_fatptrace' : 'sampledata/3_fatp_trace.xlsx',
        'dpath_fatpstation' : 'sampledata/4_fatp_station.xlsx',
        'dpath_smtrepair' : 'sampledata/4_fatp_station.xlsx',
        'dpath_smttrace_cur' : 'sampledata/6_smt_trace_current.xlsx',
        'dpath_smttrace_old' : 'sampledata/6_smt_trace_old.xlsx',
        'dpath_smttstation_cur' : 'sampledata/7_smt_station_current.xlsx',
        'dpath_smttstation_old' : 'sampledata/7_smt_station_old.xlsx',
    }
    
    @classmethod
    def get_defaults(cls, n):
        if n in cls._defaults:
            return cls._defaults[n]
        else:
            return "Unrecognized attribute name '" + n + "'"    
            
    def __init__(self, **kwargs):
        self.__dict__.update(self._defaults)
        self.__dict__.update(kwargs)
        self.initpara()
    
    def initpara(self):
        self.bigtable={}
        self.rawdata={}   
        self.factortable={}
        self.ispreprocessed=False
        
        
    def load_mesdata(self):
        self.rawdata['df_fatpTest'] = pd.read_excel(self.dpath_fatptest)
        self.rawdata['df_fatpRepair'] = pd.read_excel(self.dpath_fatprepair)
        self.rawdata['df_fatpTrace'] = pd.read_excel(self.dpath_fatptrace)
        self.rawdata['df_fatpStation'] = pd.read_excel(self.dpath_fatpstation)
        # self.rawdata['smtRepair'] = pd.read_excel(self.dpath_smtrepair)
        # self.rawdata['smtTrace_cur'] = pd.read_excel(self.dpath_smttrace_cur)
        # self.rawdata['smtTrace_old'] = pd.read_excel(self.dpath_smttrace_old)
        self.rawdata['df_smtStation_cur'] = pd.read_excel(self.dpath_smttstation_cur)
        self.rawdata['df_smtStation_old'] = pd.read_excel(self.dpath_smttstation_old)
        self.ispreprocessed=False


    #====== 組大表 =======    
    def getbigtable(self):
        self._preprocess()
        self._bigtable_joinmbsn()
        self._bigtable_fatprepair()
        self._bigtable_fatptrace()
        self._bigtable_fatpstation()
        self._bigtable_smtstation()
        self._bigtable_widetable()  
        
    ##FIXME: 需要請IT統一時間格式, 不能放在程式裡處理
    def _preprocess(self):
        '''
            - 目的: 
               - 處理空值, 空白字元, 
               - 處理日期時間相關欄位
            - df_fatpTest  
                - 欄位"是否是不良?": 需手動把NAN置換為'良'
                - SERIAL_NUMBER: str
                - IN_STATION_TIME: 轉換為datetime
            - df_fatpStation
                - SERIAL_NUMBER: str
                - IN_STATION_TIME: 轉換為datetime
            - df_fatpTrace
                - 欄位"TYPE": 有空白字元
                - 欄位"TYPE": 過濾空字元與NAN
                - SERIAL_NUMBER: str
                - WORK_TIME: 轉換為datetime
                - drop_duplicates
            - df_fatpRepair
                - 欄位"TYPE": 有空白字元
                - 欄位"TYPE": 過濾空字元與NAN
                - 欄位"SERIAL_NUMBER": str    
                - 欄位"TEST_TIME": 轉換為datetime
                - 欄位"IN_LINE_TIME": 轉換為datetime
                - 欄位"REPAIR_TIME": 轉換為datetime
            - df_smtStation_
                - 欄位"SERIAL_NUMBER": str
                - 欄位"IN_LINE_TIME": 轉換為datetime        
            *RISK:
                - 每份檔案的時間格式不一致
        '''
        if self.ispreprocessed==True:
            print('skip preprocess')
            return
        #FATP Test
        df_fatpTest = self.rawdata['df_fatpTest']
        df_fatpTest['是否是不良?'].fillna('良', inplace=True)
        df_fatpTest['SERIAL_NUMBER'] = df_fatpTest['SERIAL_NUMBER'].astype(str)
        df_fatpTest['IN_STATION_TIME']=df_fatpTest['IN_STATION_TIME'].map(lambda x: datetime.strptime(x, '%Y/%m/%d  %H:%M:%S'))
        self.rawdata['df_fatpTest'] = df_fatpTest
    
        #FATP TRACE
        df_fatpTrace = self.rawdata['df_fatpTrace']
        df_fatpTrace['TYPE'] = df_fatpTrace['TYPE'].map(lambda x: x.strip())
        df_fatpTrace = df_fatpTrace[(df_fatpTrace['TYPE'] != "") & ~(df_fatpTrace['KEY_PART_SN'].isna())]
        df_fatpTrace['SERIAL_NUMBER'] = df_fatpTrace['SERIAL_NUMBER'].astype(str)
        df_fatpTrace['WORK_TIME']=df_fatpTrace['WORK_TIME'].map(lambda x: x.replace('下午','PM').replace('上午','AM'))
        df_fatpTrace = df_fatpTrace[df_fatpTrace['WORK_TIME'].str.len()==20]
        df_fatpTrace['WORK_TIME']=df_fatpTrace['WORK_TIME'].map(lambda x: datetime.strptime(x, '%Y/%m/%d %p %H:%M:%S'))
        df_fatpTrace.drop_duplicates(inplace=True)
        self.rawdata['df_fatpTrace'] = df_fatpTrace

        #FATP STATION
        df_fatpStation = self.rawdata['df_fatpStation']
        df_fatpStation['SERIAL_NUMBER'] = df_fatpStation['SERIAL_NUMBER'].astype(str)
        df_fatpStation['IN_STATION_TIME']=df_fatpStation['IN_STATION_TIME'].map(lambda x: x.replace('下午','PM').replace('上午','AM'))
        df_fatpStation = df_fatpStation[df_fatpStation['IN_STATION_TIME'].str.len()==20]
        df_fatpStation['IN_STATION_TIME']=df_fatpStation['IN_STATION_TIME'].map(lambda x: datetime.strptime(x, '%Y/%m/%d %p %H:%M:%S'))
        self.rawdata['df_fatpStation'] = df_fatpStation

        #FATP REPAIR
        df_fatpRepair = self.rawdata['df_fatpRepair']
        df_fatpRepair = df_fatpRepair[(df_fatpRepair['TYPE'] != "") & ~(df_fatpRepair['OLD_KEY_PART_SN'].isna())]
        df_fatpRepair['SERIAL_NUMBER'] = df_fatpRepair['SERIAL_NUMBER'].astype(str)
        df_fatpRepair.dropna(subset=['SERIAL_NUMBER', 'TYPE', 'OLD_KEY_PART_SN'], inplace=True)
        for timecol in ['TEST_TIME','IN_LINE_TIME','REPAIR_TIME']:
            df_fatpRepair[timecol]=df_fatpRepair[timecol].map(lambda x: x.replace('下午','PM').replace('上午','AM'))
            df_fatpRepair = df_fatpRepair[df_fatpRepair[timecol].str.len()==20]
            df_fatpRepair[timecol]=df_fatpRepair[timecol].map(lambda x: datetime.strptime(x, '%Y/%m/%d %p %H:%M:%S'))
        self.rawdata['df_fatpRepair'] = df_fatpRepair
        
        #STM STATION
        df_smtStation_cur = self.rawdata['df_smtStation_cur']
        df_smtStation_old = self.rawdata['df_smtStation_old']
        df_smtStation_cur['SERIAL_NUMBER'] = df_smtStation_cur['SERIAL_NUMBER'].astype(str)
        df_smtStation_old['SERIAL_NUMBER'] = df_smtStation_old['SERIAL_NUMBER'].astype(str)
        df_smtStation_cur['IN_STATION_TIME']=df_smtStation_cur['IN_STATION_TIME'].map(lambda x: x.replace('下午','PM').replace('上午','AM'))
        df_smtStation_cur = df_smtStation_cur[df_smtStation_cur['IN_STATION_TIME'].str.len()>19]
        df_smtStation_cur['IN_STATION_TIME']=df_smtStation_cur['IN_STATION_TIME'].map(lambda x: datetime.strptime(x, '%Y/%m/%d %p %H:%M:%S'))
        df_smtStation_old['IN_STATION_TIME']=df_smtStation_old['IN_STATION_TIME'].map(lambda x: x.replace('下午','').replace('上午',''))
        df_smtStation_old = df_smtStation_old[df_smtStation_old['IN_STATION_TIME'].str.len()>19]
        df_smtStation_old['IN_STATION_TIME']=df_smtStation_old['IN_STATION_TIME'].map(lambda x: datetime.strptime(x, '%Y/%m/%d %p %H:%M:%S'))
        self.rawdata['df_smtStation_cur'] = df_smtStation_cur
        self.rawdata['df_smtStation_old'] = df_smtStation_old
        self.ispreprocessed=True
        
    def _bigtable_joinmbsn(self):
        '''
        - 目的: 在FATP_TEST這個資料表多新增一個MB序號的欄位
        - 作法: 
            step 1. filter part type
            step 2. 刪掉repair_time在test time之前的維修紀錄
            step 3. merge test, repair, trace
            step 4. 取正確的key part sn
                - 有維修紀錄就用OLD_KEYPART_SN, 沒有則用TRACE的SN
                    - old key part sn -> 有被維修過的UUT被換下來的MB_SN (mbsn_repair)
            step 5. 同一筆過站紀錄可能有兩筆維修紀錄, 保留時間較小的那筆
        - RISK
            - 大部分的REPAIR的TEST_TIME和TEST的IN_STATION_TIME不match
        '''
        df_fatpTrace = self.rawdata['df_fatpTrace']
        df_fatpRepair = self.rawdata['df_fatpRepair']
        df_fatpTest = self.rawdata['df_fatpTest']
        
        #step 1
        df_trace = df_fatpTrace.query("TYPE=='MB'")[['SERIAL_NUMBER','KEY_PART_SN','WORK_TIME']]
        df_repair = df_fatpRepair.query("TYPE=='MB'")[['SERIAL_NUMBER','OLD_KEY_PART_SN','REPAIR_TIME']]
        df_test = df_fatpTest[['SERIAL_NUMBER','IN_STATION_TIME','是否是不良?']]
        #step 2
        df_repair = pd.merge(df_repair, df_test, on='SERIAL_NUMBER', how='left')
        df_repair = df_repair[df_repair['REPAIR_TIME']>df_repair['IN_STATION_TIME']]
        del df_repair['是否是不良?']
        del df_repair['IN_STATION_TIME']
        #step 3
        df_merge = pd.merge(df_test, df_repair, on='SERIAL_NUMBER', how='left')
        df_merge = pd.merge(df_merge, df_trace, on='SERIAL_NUMBER', how='left')
        #step 4 
        df_merge['MB_SN']=df_merge['OLD_KEY_PART_SN'].fillna(df_merge['KEY_PART_SN'])
        #step 5
        df_merge.sort_values(by=['SERIAL_NUMBER','IN_STATION_TIME','REPAIR_TIME'], inplace=True)
        df_merge.drop_duplicates(subset=['SERIAL_NUMBER','IN_STATION_TIME'], keep='first', inplace=True)
        df_merge = df_merge[['SERIAL_NUMBER','IN_STATION_TIME','MB_SN']]
        df_testmbsn = pd.merge(df_fatpTest, df_merge, on=['SERIAL_NUMBER','IN_STATION_TIME'], how='left')
        df_testmbsn.rename(columns={'IN_STATION_TIME':'TEST_TIME'}, inplace=True)
        self.bigtable[f'test'] = df_testmbsn


    def _bigtable_fatprepair(self):
        '''
        - 目的: 抓出FATP_REPAIR裡面的TYPE, OLD_KEY_PART_SN欄位
        - 作法: IT提供的數據表已經做好對應, 只需要篩選, 暫時不另外處理
        '''
        df_fatpRepair = self.rawdata['df_fatpRepair']
        self.bigtable[f'repair'] = df_fatpRepair
        pass
    
    def _bigtable_fatptrace(self):
        '''
        - 目的: 每一個序號的組裝料件資訊
        - 作法: 
            1. filter part type
            2. 刪掉repair_time在test time之前的紀錄
            3. merge test, repair, trace
            4. 取正確的key part sn
                - 有維修紀錄就用OLD_KEYPART_SN, 沒有則用TRACE的SN
            5. 同一筆撿料紀錄可能有兩筆維修紀錄, 保留時間較小的那筆
            6. parse vendor code
            6. 重新命名欄位名稱
        - Risk: 
            - CABLE有3種, 分別在(PICK2, ASSY3, ASSY4)組裝
            - 大部分的TRACE的WORK_TIME都>TEST_TIME        
        '''
        df_fatpTrace = self.rawdata['df_fatpTrace']
        df_fatpRepair = self.rawdata['df_fatpRepair']
        df_fatpTest = self.rawdata['df_fatpTest']
        
        col_trace= ['SERIAL_NUMBER','GROUP_NAME','TYPE','KEY_PART_NO','KEY_PART_SN','WORK_TIME','VERSION']
        col_repair = ['SERIAL_NUMBER','OLD_KEY_PART_SN','REPAIR_TIME']
        col_test = ['SERIAL_NUMBER','IN_STATION_TIME','是否是不良?']
        col_output = ['SERIAL_NUMBER','TEST_TIME','是否是不良?','GROUP_NAME','KEY_PART_NO','KEY_PART_SN','WORK_TIME','VERSION']   
        #col_parse = ['country','dellpn','venderid','DC']
        col_parse = ['venderid','DC']
        col_output.extend(col_parse)
        part_types = ['HDD', 'SPEAKER', 'BATT', 'THERMAL', 'FFC', 'POWER CODE','AC ADAP', 'UPPER', 'KB', 'TOUCH PAD', 
                     'CAMERA',  'EDP CABLE', 'LCD COVER', 'LCD PANEL', 'LCD BEZEL', 'ANTENNA','LOWER', 'MB', 
                     'DC IN CABLE', 'AUDIO/B', 'BOTTOM/B','BATTERY CABLE']
        for part in part_types:
            #step 1
            df_trace = df_fatpTrace.query(f"TYPE=='{part}'")[col_trace]
            df_repair = df_fatpRepair.query(f"TYPE=='{part}'")[col_repair]
            df_test = df_fatpTest[col_test]
            df_test.rename(columns={'IN_STATION_TIME':'TEST_TIME'}, inplace=True)
            #step 2
            df_repair = pd.merge(df_repair, df_test, on='SERIAL_NUMBER', how='left')
            df_repair = df_repair[df_repair['REPAIR_TIME']>df_repair['TEST_TIME']]
            del df_repair['是否是不良?']
            del df_repair['TEST_TIME']
            #setp 3
            df_merge = pd.merge(df_test, df_repair, on='SERIAL_NUMBER', how='left')
            df_merge = pd.merge(df_merge, df_trace, on='SERIAL_NUMBER', how='left')
            #step 4
            df_merge['KEY_PART_SN']=df_merge['OLD_KEY_PART_SN'].fillna(df_merge['KEY_PART_SN'])
            #step 5
            df_merge.sort_values(by=['SERIAL_NUMBER','TEST_TIME','REPAIR_TIME'], inplace=True)
            df_merge.drop_duplicates(subset=['SERIAL_NUMBER','TEST_TIME'], keep='first', inplace=True)
            #step 6
            df_merge[f'country'] = df_merge['KEY_PART_SN'].map(lambda ksn: str(ksn)[:2]) if part!='MB' else None
            df_merge[f'dellpn'] = df_merge['KEY_PART_SN'].map(lambda ksn: str(ksn)[2:8]) if part!='MB' else None
            df_merge[f'venderid'] = df_merge['KEY_PART_SN'].map(lambda ksn: str(ksn)[8:13]) if part!='MB' else None
            df_merge[f'DC'] = df_merge['KEY_PART_SN'].map(lambda ksn: str(ksn)[8:16]) if part!='MB' else None
            df_merge[f'serialno'] = df_merge['KEY_PART_SN'].map(lambda ksn: str(ksn)[16:20]) if part!='MB' else None    
            #step 7
            df_merge = df_merge[col_output]
            _col_rename = dict(zip(col_trace, [f'part:{part}:{f}' if f!='SERIAL_NUMBER' else f for f in col_trace]))
            df_merge.rename(columns=_col_rename, inplace=True)
            _col_rename = dict(zip(col_test, [f'test:{f}' if f!='SERIAL_NUMBER' else f for f in col_test]))
            df_merge.rename(columns=_col_rename, inplace=True)
            _col_rename = dict(zip(col_parse, [f'part:{part}:{f}' if f!='SERIAL_NUMBER' else f for f in col_parse]))
            df_merge.rename(columns=_col_rename, inplace=True)
            self.bigtable[f'part:{part}'] = df_merge
    
    def _bigtable_fatpstation(self):
        '''
        - 目的: 每一個序號的FATP過站資訊
        - 作法: 
            1. filter group_name
            2. 刪掉in_station_time在test time之後的紀錄
            3. merge test, fatp_station
            4. 同一筆過站紀錄可能有兩筆過站紀錄, 保留最新的那筆
            5. 重新命名欄位名稱        
        '''
        df_fatpStation = self.rawdata['df_fatpStation']
        df_fatpTest = self.rawdata['df_fatpTest']
        
        col_test = ['SERIAL_NUMBER','IN_STATION_TIME','是否是不良?']
        col_fatpstation = ['SERIAL_NUMBER','GROUP_NAME','IN_STATION_TIME','ERROR_FLAG','LINE_NAME','STATION_NAME','EMP_NO']
        col_output = ['SERIAL_NUMBER','TEST_TIME','是否是不良?','IN_STATION_TIME','ERROR_FLAG','LINE_NAME','STATION_NAME','EMP_NO']
        for station in ['STRU','TP-2']:
            #step 1 
            df_station = df_fatpStation.query(f"GROUP_NAME=='{station}'")[col_fatpstation]
            df_test = df_fatpTest[col_test]
            df_test.rename(columns={'IN_STATION_TIME':'TEST_TIME'}, inplace=True)
            #step 2 
            df_station = pd.merge(df_test, df_station, on='SERIAL_NUMBER', how='left')
            df_station = df_station[df_station['IN_STATION_TIME']<=df_station['TEST_TIME']]
            del df_station['是否是不良?']
            del df_station['TEST_TIME']
            #step 3 
            df_merge = pd.merge(df_test, df_station, on='SERIAL_NUMBER', how='left')
            #step 4 
            df_merge.sort_values(by=['SERIAL_NUMBER','TEST_TIME','IN_STATION_TIME'], inplace=True)
            df_merge.drop_duplicates(subset=['SERIAL_NUMBER','TEST_TIME'], keep='last', inplace=True)
            #step 5
            df_merge = df_merge[col_output]
            _col_rename = dict(zip(col_fatpstation, [f'fatpstation:{station}:{f}' if f!='SERIAL_NUMBER' else f for f in col_fatpstation]))
            df_merge.rename(columns=_col_rename, inplace=True)
            _col_rename = dict(zip(col_test, [f'test:{f}' if f!='SERIAL_NUMBER' else f for f in col_test]))    
            df_merge.rename(columns=_col_rename, inplace=True)
            self.bigtable[f'fatp_station:{station}'] = df_merge    
    
    def _bigtable_smtstation(self):
        '''
        - 目的: 每一個序號的SMT過站資訊
        - 作法: 
            1. 合併cur,old
            2. filter group_name
            3. merge test, smt_station
            4. 同一筆過站紀錄可能有兩筆過站紀錄, 保留最新的那筆
            5. 重新命名欄位名稱        
        '''
        df_smtStation_cur = self.rawdata['df_smtStation_cur']
        df_smtStation_old = self.rawdata['df_smtStation_old']
        df_test = self.bigtable[f'test']
        
        col_test = ['MB_SN','TEST_TIME','是否是不良?']
        col_smtstation = ['SERIAL_NUMBER','GROUP_NAME','IN_STATION_TIME','ERROR_FLAG','LINE_NAME','STATION_NAME','EMP_NO']
        col_output = ['SERIAL_NUMBER','TEST_TIME','是否是不良?','IN_STATION_TIME','ERROR_FLAG','LINE_NAME','STATION_NAME','EMP_NO']
        for station in ['AOIA','ATE','AOIB','F/T']:
            #step 1 
            df_smtstation_cur = df_smtStation_cur.query(f"GROUP_NAME=='{station}'")[col_smtstation]
            df_smtstation_old = df_smtStation_old.query(f"GROUP_NAME=='{station}'")[col_smtstation]
            #step 2
            df_smtstation = pd.concat([df_smtstation_cur, df_smtstation_old])
            df_test = df_test[col_test]

            #step 3 
            df_smtstation = pd.merge(df_test, df_smtstation, left_on='MB_SN', right_on='SERIAL_NUMBER', how='left')
            df_smtstation = df_smtstation[df_smtstation['IN_STATION_TIME']<=df_smtstation['TEST_TIME']]
            del df_smtstation['是否是不良?']
            del df_smtstation['TEST_TIME']
            #step 4
            df_merge = pd.merge(df_test, df_smtstation, left_on='MB_SN', right_on='SERIAL_NUMBER', how='left')
            #step 5 
            df_merge.sort_values(by=['SERIAL_NUMBER','TEST_TIME','IN_STATION_TIME'], inplace=True)    
            df_merge.drop_duplicates(subset=['SERIAL_NUMBER','TEST_TIME'], keep='last', inplace=True)
            #step 6
            df_merge = df_merge[col_output]
            _col_rename = dict(zip(col_smtstation, [f'smtstation:{station}:{f}' if f!='SERIAL_NUMBER' else f for f in col_smtstation]))
            df_merge.rename(columns=_col_rename, inplace=True)
            _col_rename = dict(zip(col_test, [f'test:{f}' if f not in ['MB_SN','TEST_TIME'] else f for f in col_test]))
            df_merge.rename(columns=_col_rename, inplace=True)
            self.bigtable[f'smt_station:{station}'] = df_merge
    
    def _bigtable_smttrace(self):
        pass      
    
    def _bigtable_widetable(self):
        df_bigtable = self.bigtable['test'][['SERIAL_NUMBER','MB_SN','TEST_TIME','是否是不良?']]
        df_bigtable.rename(columns={'是否是不良?':'test:是否是不良?'}, inplace=True)
        for k,data in self.bigtable.items():
            if k in['test', 'repair']:
                continue    
            if 'smt' in k:
                data.rename(columns={'SERIAL_NUMBER':'MB_SN'}, inplace=True)
                df_bigtable = pd.merge(df_bigtable, data, left_on=['MB_SN','TEST_TIME','test:是否是不良?'], right_on=['MB_SN','TEST_TIME','test:是否是不良?'], how='left')
            else:
                df_bigtable = pd.merge(df_bigtable, data, on=['SERIAL_NUMBER','TEST_TIME','test:是否是不良?'], how='left')
        self.bigtable['big'] = df_bigtable
    
    
    #====== Factor Ranking =======
    def _getentropy(self, df, feature, isonehot=True):
        if isonehot:
            data_f = df[df[feature]==1]
            len_f1=len(data_f)
            p_data1 = data_f['label'].value_counts() 
            ent1 = sc.stats.entropy(p_data1/len_f1, base=2)
            data_f = df[df[feature]==0]
            len_f0=len(data_f)
            p_data0 = data_f['label'].value_counts()
            ent0 = sc.stats.entropy(p_data0/len_f0, base=2)

            ent_mean = ent1*(len_f1/(len_f1+len_f0))+ent0*(len_f0/(len_f1+len_f0))
            p_data1.rename(index={1:'Fail',0:'Pass'}, inplace=True)
            p_data0.rename(index={1:'Fail',0:'Pass'}, inplace=True)
            return (ent1, p_data1, ent0, p_data0, ent_mean)
        else:
            entlist=[]
            for v in df[feature].unique():
                data_f = df[df[feature]==v]
                len_f=len(data_f)
                p_data = data_f['label'].value_counts() 
                ent_ = sc.stats.entropy(p_data/len_f, base=2)
                entlist.append(ent_*len_f/len(df))
            ent = np.sum(entlist)
            return ent

    def rankfactor(self):
        df_bigtable_ = self.bigtable['big']
        ##FIXME 暫時移除時間和SN的欄位
        newcols= list(filter(lambda x: 'TIME' not in x, df_bigtable_.columns))
        newcols= list(filter(lambda x: 'KEY_PART_SN' not in x, newcols))
        df_bigtable = df_bigtable_[newcols]
        train_labels = df_bigtable['test:是否是不良?'].copy()
        train_labels.name='label'
        train_labels.replace('良',0,inplace=True)
        train_labels.replace('不良',1,inplace=True)
        train = df_bigtable.drop(columns = ['test:是否是不良?'])
        del train['SERIAL_NUMBER']
        del train['MB_SN']
        fs = FeatureSelector(data = train, labels = train_labels)
        
        #remove不能用的因子 (missing value>30%, single unique)
        fs.identify_missing(missing_threshold=0.3)
        fs.identify_single_unique()
        train = fs.remove(methods = ['missing', 'single_unique'])
        print(fs.check_removal())
        fs = FeatureSelector(data = train, labels = train_labels)        
        
        #One-Hot Encoding
        fs.identify_zero_importance(task = 'classification', eval_metric = 'auc', n_iterations = 1, early_stopping = True)
        one_hot_features = fs.one_hot_features
        base_features = fs.base_features
        print('There are %d original features' % len(base_features))
        print('There are %d one-hot features' % len(one_hot_features))        
        
        #Layer 1 factor analysis 
        ocols=['feature','Qty{P}','Qty{N}','entropy(mean)']
        df_entdata = pd.concat([fs.data_all[one_hot_features], train_labels], axis=1)
        rootent = sc.stats.entropy(df_entdata['label'].value_counts()/len(df_entdata['label']), base=2)
        print('root ent:',rootent)
        entlist = []
        for f in df_entdata.columns:
            if f=='label':
                continue
            ent1, p_data1, ent0, p_data0, ent = self._getentropy(df_entdata, f)
            #entlist.append([f, ent1, str(dict(p_data1.sort_index(ascending=False))), ent0, str(dict(p_data0.sort_index(ascending=False))), ent])
            entlist.append([f, ent1, dict(p_data1.sort_index(ascending=False)), ent0, dict(p_data0.sort_index(ascending=False)), ent])

        df_ent = pd.DataFrame(entlist)
        df_ent.columns = ['feature','entropy(P)','Qty(P)','entropy(N)','Qty(N)','entropy(mean)']
        #df_ent['exp'] = df_ent['entropy(mean)'].map(lambda x: (rootent-x)/rootent) #exp: 解釋了多少比例的不確定性
        df_ent = df_ent.sort_values(by='entropy(mean)').reset_index(drop=True)
        self.factortable['main']=df_ent
        #Layer 2 factor analysis
        topN=5
        for s, layer1 in enumerate(df_ent['feature'][:topN].values):
            groupname = layer1.split(':')[1]
            partname = layer1.split(':')[3]#s.split('_')[-1]    
            df_entdata = pd.concat([fs.data_all[one_hot_features], train_labels], axis=1)
            df_entdata=df_entdata[df_entdata[layer1]==1]
            rootent = sc.stats.entropy(df_entdata['label'].value_counts()/len(df_entdata['label']), base=2)
            entlist = []
            for f in df_entdata.columns:
                if f=='label':
                    continue
                ent1, p_data1, ent0, p_data0, ent = self._getentropy(df_entdata, f)
                entlist.append([f, ent/1, dict(p_data1.sort_index(ascending=False)), ent0, dict(p_data0.sort_index(ascending=False)), ent])
            df_ent = pd.DataFrame(entlist)
            df_ent.columns = ['feature','entropy(P)','Qty(P)','entropy(N)',f'Qty(N)','entropy(mean)']
            #df_ent['exp'] = df_ent['entropy(mean)'].map(lambda x: (rootent-x)/rootent)
            df_ent.sort_values(by='entropy(mean)', inplace=True)
            df_ent.reset_index(drop=True, inplace=True)
            self.factortable[f'{s}:{groupname}:{partname}']=df_ent


    #====== OUTPUT EXCEL REPORT ========
    def output_bigtable(self):
        writer=pd.ExcelWriter('output/bigtable.xlsx')
        for k, df in self.bigtable.items():
            k = k.replace(':',' | ')
            k = k.replace('/','')
            df.to_excel(writer,f'{k}',index=False) 
        writer.save()
        pass
    
    
    def output_factorranktable(self):
        outputcols=['Type', 'Factor A', 'Factor B', 'Factor C', 'Pass', 'Fail', 'Others-Pass', 'Others-Fail', 'entropy(mean)','rank']
        writer=pd.ExcelWriter('output/集中性分析Report.xlsx') 
        for k, df in self.factortable.items():
            #Table格式 ##FIXME
            df['Type']=df['feature'].map(lambda x: x.split(':')[0])
            df['Factor A']=df['feature'].map(lambda x: x.split(':')[1])
            df['Factor B']=df['feature'].map(lambda x: x.split(':')[2])
            df['Factor C']=df['feature'].map(lambda x: x.split(':')[3])
            del df['feature']
            df['Pass']=df['Qty(P)'].map(lambda x: x.get('Pass',0))
            df['Fail']=df['Qty(P)'].map(lambda x: x.get('Fail',0) )
            df['Others-Pass']=df['Qty(N)'].map(lambda x: x.get('Pass',0))
            df['Others-Fail']=df['Qty(N)'].map(lambda x: x.get('Fail',0) )
            del df['Qty(P)']
            del df['Qty(N)']
            del df['entropy(P)']
            del df['entropy(N)']
            df.reset_index(inplace=True)
            df.rename(columns={'index':'rank'}, inplace=True)            
            
            df = df[outputcols]
            k = k.replace(':',' | ')
            k = k.replace('/','')
            df.to_excel(writer, k, index=False)
        writer.save()
        pass


