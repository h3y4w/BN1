import os
import sys
from drivers.masterdriver import MasterDriver
class MasterV1(MasterDriver):
    model_id = "MV1"
    def __init__(self, config):
        super(MasterV1, self).__init__(config)



if __name__ == "__main__":
    MasterV1().start()
