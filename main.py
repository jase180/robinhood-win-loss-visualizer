import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from data import 

def getData(csv):
  df = pd.read_csv(csv)

def findSpreads(df):
  #iterate through each row and find Open
  #implement stack to record opens
  #if closed found, pop stack and record in results
  #calculate amount won loss for each result
  #combine trades into single result if description matches exactly and on same day
  #return results
  pass

def main():
  getData(csv)
  sortData(ascending)
  findSpreads
  printTable
  displayGraphs
  
