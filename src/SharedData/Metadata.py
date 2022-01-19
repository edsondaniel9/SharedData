import os
from pathlib import Path
import pandas as pd
import numpy as np
import time
from subprocess import run, PIPE

class Metadata():
    
    def __init__(self, name, config):
        self.name = name
        self.logger = config.logger
        self.config = config
        self.xls = {}
        self.static = pd.DataFrame([])        
        self.symbols = pd.DataFrame([],dtype=str)
        self.series = pd.Series([],dtype=str)
        self.fpath = Path(self.config.db_directory)

        self.xlsxpath = self.fpath /  ('Metadata/'+name+'.xlsx')
        self.path = self.fpath /  ('Metadata/'+name+'.pkl')
        self.pathsymbols = self.fpath /  ('Metadata/'+name+'_SYMBOLS.pkl')
        self.pathseries = self.fpath /  ('Metadata/'+name+'_SERIES.pkl')

        if not os.path.isdir(self.path.parents[0]):
            os.makedirs(self.path.parents[0]) 

        if (self.config.s3_sync):
           
            folder=str(self.path.parents[0]).replace(self.config.db_directory,'')
            folder = folder.replace('\\','/')+'/'
            dbfolder = str(self.path.parents[0])
            dbfolder = dbfolder.replace('\\','/')+'/'
            awsfolder = self.config.s3_bucket + folder
            awsclipath = self.config.awsclipath
            result = run([awsclipath,'s3','sync',awsfolder,dbfolder,\
                '--exclude="*"',
                '--include="'+name+'.xlsx,'+name+'.pkl,'+name+'/FUT/SYMBOLS.pkl,'+name+'/FUT/SERIES.pkl"']\
                ,shell=True, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            print(result.returncode, result.stdout, result.stderr)

        if self.path.is_file():            
            tini = time.time()
            # logging.info('Loading symbols collection ' + name + ' ...')
            self.static = pd.read_pickle(self.path)
            self.static = self.static.sort_index()
            if self.pathsymbols.is_file():
                self.symbols = pd.read_pickle(self.pathsymbols)
                self.symbols = self.symbols.sort_index()
            if self.pathseries.is_file():
                self.series = pd.read_pickle(self.pathseries)
                self.series = self.series.sort_index()
            # logging.info('%.2f done!' % (time.time()-tini))     
        else:            
            if self.xlsxpath.is_file():            
                tini = time.time()
                # logging.info('Loading symbols collection ' + name + ' ...')
                self.xls = pd.read_excel(self.xlsxpath,sheet_name=None)
                self.static = self.xls['static']
                if 'symbols' in self.xls.keys():
                    self.symbols = self.xls['symbols']
                if not self.static.empty:
                    self.static = self.static.set_index(self.static.columns[0])
                if not self.symbols.empty:
                    self.symbols = self.symbols.set_index(self.symbols.columns[0])
                # logging.info('%.2f done!' % (time.time()-tini))

    def save(self,save_excel=False):
        tini = time.time()
        # logging.info('Saving symbols collection ' + self.name + ' ...')  
        if not os.path.isdir(self.path.parents[0]):
            os.makedirs(self.path.parents[0])                          
        self.static.to_pickle(self.path)
        if not self.symbols.empty:
            self.symbols.to_pickle(self.pathsymbols)
        if not self.series.empty:
            self.series.to_pickle(self.pathseries)
        if save_excel:
            writer = pd.ExcelWriter(self.xlsxpath, engine='xlsxwriter')            
            self.static.to_excel(writer,sheet_name='static')        
            if not self.symbols.empty:
                self.symbols.to_excel(writer,sheet_name='symbols')
            writer.save()        
        # logging.info('%.2f done!' % (time.time()-tini))
    
    def mergeUpdate(self,newdf):
        ddidx = newdf.index.duplicated()
        if ddidx.any():
            newdf = newdf[~newdf.index.duplicated(keep='first')]
            # logging.warning('Collection merge duplicated index for new dataframe!')
        ddidx = self.static.index.duplicated()
        if ddidx.any():
            self.static = self.static[~self.static.index.duplicated(keep='first')]
            # logging.warning('Collection merge duplicated index for static dataframe!')
        newcolsidx = ~newdf.columns.isin(self.static.columns)
        if newcolsidx.any():
            newcols = newdf.columns[newcolsidx]
            for c in newcols:
                self.static.loc[:,c] = newdf[c]                
        newidx = ~newdf.index.isin(self.static.index)
        if newidx.any():
            self.static = self.static.reindex(index=self.static.index.union(newdf.index))
        newcol = ~newdf.columns.isin(self.static.columns)
        if newcol.any():
            self.static = self.static.reindex(columns=self.static.columns.union(newdf.columns))
        self.static.update(newdf)
