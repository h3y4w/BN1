import os
import sys
import bot
class MasterV1(bot.MasterDriver):
    model_id = "MV1"
    def __init__(self, config):
        super(MasterV1, self).__init__(config)



if __name__ == "__main__":
    MasterV1().start()
