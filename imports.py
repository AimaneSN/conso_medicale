import findspark
findspark.init()
import pyspark
import openpyxl
import os

abspath = os.path.abspath(__file__) #Chemin absolu du script exécuté
dname = os.path.dirname(abspath) #Chemin du dossier contenant le script
os.chdir(dname)

import colnames
import coldict
import readfile
import traitements

from pyspark.sql import SparkSession, Row, HiveContext, Window
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from joblibspark import register_spark
import pyspark.pandas as ps
import unidecode #unicode to ASCII

import  pyspark.sql.functions  as fy
import pandas as pd
import scipy as scp
from pandasql import sqldf
import matplotlib.pyplot as plt
import cvxpy as cp
import cplex
import cylp
#from tkinter import *

import numpy as np
import re as re
#from pandas_profiling import ProfileReport
import inspect
import sys
import gc
import datetime

import pyspark.ml as pyml

import math
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Bucketizer
from pyspark.ml.stat import KolmogorovSmirnovTest, ChiSquareTest
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans

from sklearn import preprocessing
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_curve
from sklearn.preprocessing import OrdinalEncoder
from sklearn.linear_model import BayesianRidge, Ridge, LinearRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.experimental import enable_iterative_imputer  
from sklearn.kernel_approximation import Nystroem
from sklearn.impute import SimpleImputer, IterativeImputer
from sklearn.pipeline import make_pipeline
import sklearn.ensemble as sk_e
from sklearn.model_selection import cross_val_score
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.neighbors import KNeighborsRegressor, KNeighborsClassifier
import sklearn

import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
import seaborn
