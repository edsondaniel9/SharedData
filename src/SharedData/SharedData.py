import os
from dotenv import load_dotenv

from SharedData.SharedDataFeeder import SharedDataFeeder
from SharedData.Metadata import Metadata
from SharedData.Logger import Logger

class SharedData:

    def __init__(self, database, mode='rw'):
        if Logger.log is None:
            load_dotenv()  # take environment variables from .env.
            Logger(os.environ('PYTHONPATH')+'\SharedData.py')

        self.database = database

        if mode == 'local':
            self.s3read = False
            self.s3write = False
        elif mode == 'r':
            self.s3read = True
            self.s3write = False
        elif mode == 'rw':
            self.s3read = True
            self.s3write = True
        
        # DATA DICTIONARY
        # SharedDataTimeSeries: data[feeder][period][tag] (date x symbols)
        # SharedDataFrame: data[feeder][period][date] (symbols x tags)
        self.data = {} 

        # Symbols collections metadata
        self.metadata = {}
        
        # DATASET
        md = Metadata('DATASET/DATASET_' + database)
        self.dataset = md.static

    def __setitem__(self, feeder, value):
        self.data[feeder] = value
                
    def __getitem__(self, feeder):        
        if not feeder in self.data.keys():
            self.data[feeder] = SharedDataFeeder(self, feeder)
        return self.data[feeder]

    def getMetadata(self, collection):
        if not collection in self.metadata.keys():              
            self.metadata[collection] = Metadata(collection)
        return self.metadata[collection]

    def getSymbols(self, collection):        
        return self.getMetadata(collection).static.index.values
    
    