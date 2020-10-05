
import os, warnings
warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from keyfactordiag.diag import Diag
import ipywidgets as widgets
from IPython.display import display

pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 10000)
pd.set_option("display.max_colwidth", 100)


class UI_DEMO(object):
    _defaults = {
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
        #--- init ---
        self.diag = Diag(errorcode='4I31', modelname='DGE00')
        self.diag.load_mesdata()
        self.diag.getbigtable()
        self.diag.get_analysis_table()
        self.entropy_tb_lv1 = self.diag.entorpy_analysis(filter_factor='All')
        self.entropy_tb_lv2 = None        
    
    
    def dropdown_factor_eventhandler(self, change):
        option= change.new.split('__')[1]
        self.entropy_tb_lv2 = self.diag.entorpy_analysis(filter_factor=option)
        self.common_data_filter(change.new, self.floatSilder_faterate.value, self.factortype_selects.value)

    def select_factortype_eventhandler(self, change):
        self.common_data_filter(self.checkbox_keyfactor.value, self.floatSilder_faterate.value, change.new)

    def checkbox_keyfactor_eventhandler(self, change):
        self.common_data_filter(change.new, self.floatSilder_faterate.value, self.factortype_selects.value)

    def floatSilder_faterate_eventhandler(self, change):
        self.common_data_filter(self.checkbox_keyfactor.value, change.new, self.factortype_selects.value)

    def common_data_filter(self, key_factor, failrate, factor_types):
        factor_types = list(factortype_selects.value)
        #--- TAB 1 ---
        output_factor_lv1.clear_output()
        entropy_tb_lv1_filter = self.entropy_tb_lv1
        entropy_tb_lv2_filter = self.entropy_tb_lv2
        if key_factor:
            entropy_tb_lv1_filter = entropy_tb_lv1_filter[entropy_tb_lv1_filter['key_factor']=='1']
        entropy_tb_lv1_filter = entropy_tb_lv1_filter[entropy_tb_lv1_filter['Failrate']>=failrate]
        #if len(factor_types)>0:
        if factor_types !=['All']:
            entropy_tb_lv1_filter = entropy_tb_lv1_filter[entropy_tb_lv1_filter['Factor B'].isin(factor_types)]

        with self.output_factor_lv1:
            print(f'Case 0: ATE R5209壓件')
            display(entropy_tb_lv1_filter)     

        #--- TAB 2 ---
        self.output_factor_lv2.clear_output()        

        if entropy_tb_lv2_filter is None:
            return
        if key_factor:
            entropy_tb_lv2_filter = entropy_tb_lv2_filter[entropy_tb_lv2_filter['key_factor']=='1']
        entropy_tb_lv2_filter = entropy_tb_lv2_filter[entropy_tb_lv2_filter['Failrate']>=failrate]
        #if len(factor_types)>0:
        if factor_types !=['All']:
            entropy_tb_lv2_filter = entropy_tb_lv2_filter[entropy_tb_lv2_filter['Factor B'].isin(factor_types)]    
        with self.output_factor_lv2:
            print(f'Case 0: ATE R5209壓件')
            print(f'selected factor:{self.dropdown_factor.value}')
            display(entropy_tb_lv2_filter)     



    def get_dashboard(self):
        item_layout = widgets.Layout(margin='0 0 50px 0')

        #lv2 factor dropdown
        options = list(self.entropy_tb_lv1['factor'].values)
        options = [f'{idx}__{op}'for idx, op in enumerate(options)]
        self.dropdown_factor = widgets.Dropdown(options = options, description='LV2 Factor', value=None)
        self.dropdown_factor.observe(self.dropdown_factor_eventhandler, names='value')

        factortype_options = list(self.entropy_tb_lv1['Factor B'].unique())
        factortype_options.insert(0,'All')
        self.factortype_selects = widgets.SelectMultiple(options=factortype_options, description='Factor Types', value=['All'])
        self.factortype_selects.observe(self.select_factortype_eventhandler, names='value')

        #filter
        self.floatSilder_faterate = widgets.FloatSlider(min=0, max=1, step=0.05, description='fail rate:',value=0)
        self.floatSilder_faterate.observe(self.floatSilder_faterate_eventhandler, names='value')
        self.checkbox_keyfactor = widgets.Checkbox(description='key_factor:',value=False)
        self.checkbox_keyfactor.observe(self.checkbox_keyfactor_eventhandler, names='value')


        input_widgets = widgets.HBox([self.dropdown_factor, self.factortype_selects, self.checkbox_keyfactor, self.floatSilder_faterate])


        #output
        self.output_factor_lv1 = widgets.Output()
        self.output_factor_lv2 = widgets.Output()
        tab = widgets.Tab([self.output_factor_lv1, self.output_factor_lv2], layout=item_layout)
        tab.set_title(0, 'main')
        tab.set_title(1, 'Lv2 Table')

        with self.output_factor_lv1:
            print(f'Case 0: ATE R5209壓件')
            display(self.entropy_tb_lv1)

        dashboard = widgets.VBox([input_widgets, tab])

        return dashboard

    
    