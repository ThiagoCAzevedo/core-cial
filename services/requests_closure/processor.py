"""
código responsável por tratar e retornar um df da lt22.txt
"""
from helpers.data.cleaner import CleanerBase
from dotenv import load_dotenv
import os


class CleanDataFrame(CleanerBase):
    def __init__(self):
        CleanerBase().__init__(self)

    def remove_initial_lines(self):
        pass