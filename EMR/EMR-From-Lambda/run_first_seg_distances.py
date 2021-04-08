# to use from Zepplin:
# sudo nano /mnt/var/lib/zeppelin/CustomerSegmentationExploration/customer-segmentation/customer_segmentation/
# paste in this file
import os
import sys
import datetime
import json
from math import floor
from datetime import timedelta
from operator import itemgetter
import dateutil.relativedelta
import pandas as pd
import numpy as np
import math
import argparse
import requests
import s3fs
import boto3
import subprocess as sub
from scipy.spatial.distance import pdist, squareform, euclidean

from pyspark import SparkContext, SparkConf # redundant in pyspark shell
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window, DataFrame
from pyspark.sql.types import LongType, DoubleType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, TimestampType, FloatType, DateType
import pyspark.sql.functions as F
from pyspark.sql.functions import size, mean, lit, datediff, count, when, isnan, udf, col, sum, log, stddev, rank, countDistinct, array, explode, struct, row_number, desc, asc, concat, split
from pyspark.ml.feature import Bucketizer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.stat import Correlation
from functools import reduce  # For Python 3.x

# file_dir = os.path.dirname(__file__)
# sys.path.append(file_dir)
s3 = s3fs.S3FileSystem(anon=False)


# CONSTANTS
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

MAX_MINUTES_PER_DAY = 600
MAX_TITLES_VIEWED = 1000
GENRE_HABITS_SCHEMA = StructType(
        [StructField('customer_id',  LongType(), True),
        StructField('encrypted_customer_id',  StringType(), True),
        StructField('minutes_viewed', DoubleType(), True),
        StructField('titles_viewed',  IntegerType(), True),
        StructField('minutes_weekday_sleep',  DoubleType(), True),
        StructField('minutes_weekday_morning',  DoubleType(), True),
        StructField('minutes_weekday_afternoon',  DoubleType(), True),
        StructField('minutes_weekday_night',  DoubleType(), True),
        StructField('minutes_weekend',  DoubleType(), True),
        StructField('minutes_tv',  DoubleType(), True),
        StructField('minutes_movie',  DoubleType(), True),
        StructField('minutes_SVOD',  DoubleType(), True),
        StructField('minutes_TVOD',  DoubleType(), True),
        StructField('minutes_living_room', DoubleType(), True),
        StructField('minutes_mobile', DoubleType(), True),
        StructField('minutes_web',  DoubleType(), True),
        StructField('avg_awards',  DoubleType(), True),
        StructField('avg_ratings',  DoubleType(), True),
        StructField('freq_ratings',  DoubleType(), True),
        StructField('action', DoubleType(), True),
        StructField('adult', DoubleType(), True),
        StructField('adventure', DoubleType(), True),
        StructField('animation', DoubleType(), True),
        StructField('biography', DoubleType(), True),
        StructField('comedy', DoubleType(), True),
        StructField('crime', DoubleType(), True),
        StructField('documentary', DoubleType(), True),
        StructField('drama', DoubleType(), True),
        StructField('family', DoubleType(), True),
        StructField('fantasy', DoubleType(), True),
        StructField('game_show', DoubleType(), True),
        StructField('history', DoubleType(), True),
        StructField('horror', DoubleType(), True),
        StructField('music', DoubleType(), True),
        StructField('musical', DoubleType(), True),
        StructField('mystery', DoubleType(), True),
        StructField('news', DoubleType(), True),
        StructField('reality_tv', DoubleType(), True),
        StructField('romance', DoubleType(), True),
        StructField('sci_fi', DoubleType(), True),
        StructField('short', DoubleType(), True),
        StructField('sport', DoubleType(), True),
        StructField('talk_show', DoubleType(), True),
        StructField('thriller', DoubleType(), True),
        StructField('war', DoubleType(), True),
        StructField('western', DoubleType(), True),
        StructField('caper', DoubleType(), True),
        StructField('soap', DoubleType(), True),
        StructField('anthology', DoubleType(), True),
        StructField('world_creation', DoubleType(), True),
        StructField('african_american', DoubleType(), True),
        StructField('middle_east', DoubleType(), True),
        StructField('lgbt', DoubleType(), True),
        StructField('female_lead', DoubleType(), True),
        StructField('dystopia', DoubleType(), True),
        StructField('anime', DoubleType(), True),
        StructField('legal', DoubleType(), True),
        StructField('police', DoubleType(), True),
        StructField('psychological', DoubleType(), True),
        StructField('political', DoubleType(), True),
        StructField('satire', DoubleType(), True),
        StructField('disaster', DoubleType(), True),
        StructField('dark_humor', DoubleType(), True),
        StructField('spy', DoubleType(), True),
        StructField('superhero', DoubleType(), True),
        StructField('dark', DoubleType(), True),
        StructField('supernatural', DoubleType(), True),
        StructField('big_city', DoubleType(), True),
        StructField('based_on_lit', DoubleType(), True),
        StructField('outer_space', DoubleType(), True),
        StructField('based_on_true', DoubleType(), True),
        StructField('young_adults', DoubleType(), True),
        StructField('period_1900s', DoubleType(), True),
        StructField('period_before_1900', DoubleType(), True),
        StructField('minutes_english_audio',  DoubleType(), True),
        StructField('minutes_french_audio',  DoubleType(), True),
        StructField('minutes_spanish_audio',  DoubleType(), True),
        StructField('minutes_german_audio',  DoubleType(), True),
        StructField('minutes_japanese_audio',  DoubleType(), True),
        StructField('minutes_hindi_audio',  DoubleType(), True),
        StructField('minutes_telugu_audio',  DoubleType(), True),
        StructField('minutes_tamil_audio',  DoubleType(), True),
        StructField('minutes_italian_audio',  DoubleType(), True),
        StructField('minutes_portuguese_audio',  DoubleType(), True),
        StructField('minutes_unknown_audio',  DoubleType(), True),
        StructField('days_since_first_stream', DoubleType(), True),
        StructField('distinct_streaming_days', IntegerType(), True),
        StructField('minutes_per_day', DoubleType(), True)
        ])
SEG_DIST_PROB_SCHEMA = StructType(
                        [StructField('customer_id',  LongType(), True),
                        StructField('is_high_streamer', IntegerType(), True),
                        StructField('sid', StringType(), True),
                        StructField('distance', DoubleType(), True),
                        StructField('probability', DoubleType(), True)
                                    ])
HABIT_GENRE_UNIFY_SEG_SCHEMA = StructType(
                                [StructField('customer_id',  LongType(), True),
                                StructField('segment_habit_id', StringType(), True),
                                StructField('segment_genre_id', StringType(), True),
                                StructField('is_high_streamer', IntegerType(), True),
                                StructField('profile_begin', StringType(), True),
                                StructField('profile_end', StringType(), True)
                                ])   
STREAM_SCHEMA_TITLES = StructType(
        [
        StructField('customer_id',  LongType(), True),
        StructField('content_type', StringType(), True),
        StructField('tconst', StringType(), True),
        StructField('gti', StringType(), True),
        StructField('title',  StringType(), True),
        StructField('seconds_viewed',  DoubleType(), True)
        ])
SEGMENT_SCHEMA = StructType(
        [StructField('marketplace_id', IntegerType(), True)
        ,StructField('country_name', StringType(), True)
        ,StructField('customer_id', LongType(), True)
        ,StructField('segment_genre_id', IntegerType(), True)
        ,StructField('segment_habit_id', IntegerType(), True)
        ,StructField('is_high_streamer', IntegerType(), True)
        ,StructField('genre_quartile', IntegerType(), True)
        ,StructField('habit_quartile', IntegerType(), True)
        ])
PREV_SEGLABEL_SCHEMA = StructType(
        [StructField('marketplace_id',  IntegerType(), True),
        StructField('country_code',  StringType(), True),
        StructField('customer_id',  LongType(), True),
        StructField('segment_genre_id_prev', IntegerType(), True),
        StructField('segment_habit_id_prev', IntegerType(), True),
        StructField('is_high_streamer_prev', IntegerType(), True),
        StructField('genre_prototypical_quartile_prev',  IntegerType(), True),
        StructField('habit_prototypical_quartile_prev',  IntegerType(), True)
        ])
CURR_SEGLABEL_SCHEMA = StructType(
        [StructField('marketplace_id',  IntegerType(), True),
        StructField('country_code',  StringType(), True),
        StructField('customer_id',  LongType(), True),
        StructField('segment_genre_id_curr', IntegerType(), True),
        StructField('segment_habit_id_curr', IntegerType(), True),
        StructField('is_high_streamer_curr', IntegerType(), True),
        StructField('genre_prototypical_quartile_curr',  IntegerType(), True),
        StructField('habit_prototypical_quartile_curr',  IntegerType(), True)
        ])
PERCENT_FEATURES = [
    'action', 'adult', 'adventure', 'animation', 'biography', 'comedy', 'crime', 'documentary', 'drama', 'family',
    'fantasy', 'game_show', 'history', 'horror', 'music', 'musical', 'mystery', 'news', 'reality_tv', 'romance',
    'sci_fi', 'short', 'sport', 'talk_show', 'thriller', 'war', 'western', 'caper', 'soap', 'anthology',
    'world_creation', 'african_american', 'middle_east', 'lgbt', 'female_lead', 'dystopia', 'anime', 'legal',
    'police', 'psychological', 'political', 'satire', 'disaster', 'dark_humor', 'spy', 'superhero', 'dark', 
    'supernatural', 'big_city', 'based_on_lit', 'outer_space', 'based_on_true', 'young_adults', 'period_1900s', 
    'period_before_1900', 'minutes_weekday_sleep', 'minutes_weekday_morning', 'minutes_weekday_afternoon',
    'minutes_weekday_night', 'minutes_weekend', 'minutes_living_room', 'minutes_mobile', 'minutes_web', 
    'minutes_tv', 'minutes_movie', 'minutes_SVOD', 'minutes_TVOD', 'minutes_english_audio', 'minutes_french_audio',
    'minutes_spanish_audio', 'minutes_german_audio', 'minutes_japanese_audio', 'minutes_hindi_audio', 'minutes_telugu_audio',
    'minutes_tamil_audio', 'minutes_italian_audio', 'minutes_portuguese_audio','minutes_unknown_audio']
LOGGED_FEATURES = ['minutes_viewed', 'titles_viewed', 'freq_ratings', 'minutes_per_day', 
                   'distinct_streaming_days', 'days_active_rate']
TRANSFORM_FEATURES = ['minutes_viewed', 'titles_viewed', 'avg_awards', 'avg_ratings',
                    'freq_ratings', 'minutes_per_day', 'distinct_streaming_days', 'days_active_rate']
HABITS = ['titles_viewed', 'minutes_per_day', 'days_active_rate', 'minutes_weekday_morning','minutes_weekday_afternoon',
          'minutes_weekday_night','minutes_weekday_sleep','minutes_weekend','minutes_living_room','minutes_mobile',
          'minutes_web','minutes_tv','minutes_movie','minutes_SVOD','minutes_TVOD','freq_ratings','avg_ratings','avg_awards']
GENRES = ['action', 'adult', 'adventure', 'animation', 'biography', 'comedy', 'crime', 'documentary', 'drama',
          'family', 'fantasy', 'game_show', 'history', 'horror', 'music', 'musical', 'mystery', 'news',
          'reality_tv', 'romance', 'sci_fi', 'short', 'sport', 'talk_show', 'thriller', 'war', 'western',
          'caper', 'soap', 'anthology', 'world_creation', 'african_american', 'middle_east', 'lgbt', 'female_lead',
          'dystopia', 'anime', 'legal', 'police', 'psychological', 'political', 'satire', 'disaster',
          'dark_humor', 'spy', 'superhero', 'dark', 'supernatural', 'big_city', 'based_on_lit', 'outer_space',
          'based_on_true', 'young_adults', 'period_1900s', 'period_before_1900']
PATH = 's3://studiosresearch-projects/customerSegmentation/redshift-unloads/CUSTOMER_GENRE_HABIT_LANG_PROFILE_MKT'
CENTROID_PATH = 's3://studiosresearch-projects/customerSegmentation/centroids/'
SEG_DIST_PROB_PATH = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/'
SSEKMSKEYID="arn:aws:kms:us-east-1:934181291337:key/14aaca1e-d8ae-49fb-86a7-2f6fcf25bd74"

# Initialize Kmeans parameters
SEED = 123
INIT_STEPS = 250
MAX_ITER = 100
TOL = 0.000001


# UTILS
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def gather_data(ss, schema=None, s3_path=None, delimiter="\t"):
        """
        Opens S3 protocol and retrieves data

        PARAMETERS: 
            schema - column/feature labels for particular data
            path - path data

        RETURN
        df - data for modeling
        """
        if not s3_path:
            print('Please tell me a path!')
            return

        # Initialize S3 file
        fs = s3fs.S3FileSystem()
        # Resync with S3
        sub.call(['emrfs', 'delete', s3_path])
        sub.call(['emrfs', 'sync', s3_path])

        # Read data from S3
        print("Reading Data from: %s" %(s3_path))
        if schema:
            df = ss.read.format("com.databricks.spark.csv")\
                                        .option("header", "false")\
                                        .option("inferSchema", "false")\
                                        .option("delimiter", delimiter)\
                                        .load(s3_path, schema=schema)\
                                        .cache()
        else:
            df = ss.read.format("com.databricks.spark.csv")\
                                        .option("header", "true")\
                                        .option("inferSchema", "true")\
                                        .option("delimiter", delimiter)\
                                        .load(s3_path)\
                                        .cache()
        
        if 'redshift-unloads' in s3_path:
            # drop unsycnced customers if any, not that many
            df = df.filter(df['customer_id'].isNotNull()) 
        
        return df

def compute_distance(df, cluster_centers, cluster_ids, dist_func, link_func):
    """
    Runs distance func and appends new column for dist between each customer and K cluster center/centroid
    """
    for centroid, cluster_id in zip(cluster_centers, cluster_ids):
        # Initialize the distance udf
        distance_udf = udf(lambda x: float(dist_func(x, centroid)), FloatType()) # take inverse
        # Add new distance column
        col_name = 'dist_to_c_%s' %cluster_id
        df = df.withColumn(col_name, distance_udf(col('features')))
        # Transform and add new distance column
        link_col_name = 'link_dist_to_c_%s' %cluster_id
        if link_func == 'inverse':
            df = df.withColumn(link_col_name, 1. / col(col_name))
        elif link_func == 'sqre_inverse':
            df = df.withColumn(link_col_name, 1. / col(col_name) ** 2)
        elif link_func == 'sqrt_inverse':
            df = df.withColumn(link_col_name, 1. / col(col_name) ** 0.5)

    return df

def melt(df, id_vars, value_vars, var_name, value_name):
    """
    Convert DataFrame from wide to long format
    """
    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

def generate_cluster_distance_prob(df, cluster_centers, cluster_ids, dist_func, link_func):
    """
    Generates probabilities based on distance

    PARAMETERS:
        df - raw customer level data
        cluster_centers - K cluster centroids, FEATURES ONLY, no labels or indicies
        cluster_ids - K cluster ids
        dist_func - metric of similarity (e.g. KMeans())
        link_func - transform function to reinterpret distance to represent higher is better

    RETURNS:
        seg - final optimal segment per customer
        seg_dist_prob - multiple rows per customer, representing each distance to centroid
    """
    # Compute and add distance columns to each cluster centroid, (e.g. if there are 4 clusters, then add 4 rows)
    df_dist = compute_distance(df, cluster_centers, cluster_ids, dist_func, link_func)
    df_dist.cache()

    # Filter features
    dist_to_cluster_cols = ["dist_to_c_%s" %k for k in cluster_ids]
    link_dist_to_cluster_cols = ["link_dist_to_c_%s" %k for k in cluster_ids]
    cluster_cols = ["%s" %k for k in cluster_ids]
    main_df = df_dist.select(['customer_id', 
                              'high_streamer'] + 
                              dist_to_cluster_cols + 
                              link_dist_to_cluster_cols)
    main_df = main_df.withColumn('high_streamer', col('high_streamer').cast(IntegerType()))
    
    # Compute total distance to be able to compute probability
    main_df = main_df.withColumn('total', np.sum(col(c) for c in link_dist_to_cluster_cols)) # sums across columns
    for c_d, c_dl, c in zip(dist_to_cluster_cols, link_dist_to_cluster_cols, cluster_cols):
        main_df = main_df.withColumn(c_dl, col(c_dl) / col('total')) # compute probability
        main_df = main_df.withColumn(c, concat(col(c_d), lit("_"), col(c_dl))) # concat distance_probability
    main_df.cache()

    # Extract the centroid with minimum distance, rowwise
    schema = StructType([StructField('minval', FloatType()), StructField('minval_colname', StringType())])
    mincol = udf(lambda row: min(row, key=itemgetter(0)), schema)
    seg = main_df.withColumn('minfield', 
                             mincol(struct([struct(main_df[x], lit(x)) for x in dist_to_cluster_cols]))
                            ).select('customer_id', 
                                    col('high_streamer').alias('is_high_streamer'), 
                                    col('minfield.minval_colname').alias('pred_no1'))
    seg = seg.withColumn('pred_no1', split(seg['pred_no1'], '_').getItem(3)) 
    
    # Reformat dataframe to have multiple rows per customer-cluster
    seg_dist_prob_full = melt(main_df, 
                             id_vars = ['customer_id', 'high_streamer'], 
                             value_vars = cluster_cols,
                             var_name='cluster_id', 
                             value_name='probability')
    seg_dist_prob_full.cache()

    # Pull out the distance and probability into separate columns
    seg_dist_prob_full = seg_dist_prob_full.withColumn('distance', 
                                                        split(seg_dist_prob_full['probability'], '_').getItem(0).cast(DoubleType()))
    seg_dist_prob_full = seg_dist_prob_full.withColumn('probability', 
                                                        split(seg_dist_prob_full['probability'], '_').getItem(1).cast(DoubleType()))

    # Finally prepare distance df with, comparing all centroids
    seg_dist_prob = seg_dist_prob_full.select('customer_id',
                                            col('high_streamer').alias('is_high_streamer'),
                                            col('cluster_id').alias('sid'),
                                            'distance', 'probability')
    
    return seg, seg_dist_prob

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


# SEGMENTATION
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

class Segmentation:
    """
    Runs Customer Segmentation

    PARAMETERS: 
        genre_habit_raw_data - all data
        features - features to train KMeans
    """
    def __init__(self, genre_habit_raw_data, features):
        self.genre_habit_raw_data = genre_habit_raw_data
        self.features = features
        self.high_data = None
        self.low_data = None
    
    def transform_data(self):
        """
        Transforms all data

        PARAMETERS: 
            genre_habit_raw_data
        
        RETURN
            high_data - active users
            low_data - not as active users
        """
        # make a copy
        raw_data = self.genre_habit_raw_data

        # create days_active feature
        raw_data = raw_data.withColumn('days_active_rate', raw_data.distinct_streaming_days / raw_data.days_since_first_stream)
        # fix NAs
        mean_ratings = raw_data.agg(mean('avg_ratings')).collect()[0][0]
        raw_data = raw_data.na.fill({'avg_awards': 0.0, 
                                     'freq_ratings': 0.0, 
                                     'avg_ratings': mean_ratings})
        # create flag for high streamer
        median_minutes_viewed = raw_data.stat.approxQuantile('minutes_viewed', [0.5], 0.001)[0] 
        median_titles_viewed = raw_data.stat.approxQuantile('titles_viewed', [0.5],  0.001)[0]
        # Filter dataset first
        raw_data = raw_data.filter((raw_data.minutes_per_day < MAX_MINUTES_PER_DAY) & \
                                    (raw_data.titles_viewed < MAX_TITLES_VIEWED))
        # Flag high streamers
        raw_data = raw_data.withColumn("high_streamer", 
                                    ((raw_data.minutes_viewed >= median_minutes_viewed) & \
                                    (raw_data.titles_viewed >= median_titles_viewed)
                                    ))
        # make genre percent features
        print('Transforming features to percent...')
        for feat in PERCENT_FEATURES:
            raw_data = raw_data.withColumn(feat, col(feat) / col("minutes_viewed") * 100)
        # make viewing percent features
        print('Logging features...\n')
        for feat in LOGGED_FEATURES:
            raw_data = raw_data.withColumn(feat, log(col(feat) + 0.001))
        
        # Redefine streamers
        high_data = raw_data.filter(raw_data.high_streamer == True)
        low_data =  raw_data.filter(raw_data.high_streamer == False)
        
        # rescale to 100
        max_new = 100
        min_new = 0
        range_new = max_new - min_new
        for feature in TRANSFORM_FEATURES:
            max_old = high_data.agg(F.max(feature)).collect()[0][0]
            min_old = high_data.agg(F.min(feature)).collect()[0][0]
            range_old = max_old - min_old
            
            high_data = high_data.withColumn(feature, 
                                                        ((col(feature) - max_old) / range_old) * range_new + max_new) 
            low_data = low_data.withColumn(feature, 
                                                        ((col(feature) - max_old) / range_old) * range_new + max_new)
        
        before_high = high_data.count()
        before_low = low_data.count()

        self.high_data = high_data.dropna()
        self.low_data = low_data.dropna()
        
        print("High Dropped %d rows" %(before_high - high_data.count()))
        print("Low Dropped %d rows" %(before_low - low_data.count()))
    
    def add_modeling_feature(self, data):
        """
        Assembles and adds genre and habits feature for modeling

        RETURNS: same data with additional feature columns for genres and habits
        """
        vec_genres_assembler = VectorAssembler(inputCols = self.features, 
                                               outputCol = "features")
        data = vec_genres_assembler.transform(data)

        return data
    
    def fit_kmeans(self, k_cluster, data, estimator):
        """
        Fits a single Kmeans model

        PARAMETERS:
            k_cluster - number of clusters
            data - customer data along with genres and habits
            estimator - Kmeans algorithm

        RETURNS: 
        model - fitted kmeans model
            transformed_data = dataset with new prediction of cluster affinity
        """
        print("Fitting Cluster %d" %(k_cluster))
        kmeans = estimator.setK(k_cluster) \
                          .setSeed(123) \
                          .setFeaturesCol('features')
        model = kmeans.fit(data)
        transformed_data = model.transform(data)
        
        return model, transformed_data

    def generate_kmeans_metrics(self, model, transformed_data, estimator, k_cluster, pred_col):
        """
        Creates metrics on the clustering output
        
        PARAMETERS:
            model - Kmeans engine
            transformed_data - data after it has been transformed by Kmeans
        """
        centers = model.clusterCenters()
        kcenters_df = pd.DataFrame(centers, columns = self.features)
        kcenters_df['center_id'] = np.arange(k_cluster) # order is correct
        kcenters_df['k_cluster'] = k_cluster
        algo_name = estimator.__str__().split("_")[0]
        kcenters_df['algo'] = algo_name
        dists = pdist(centers,'euclidean')
        kcenters_df['min_centroid_dist'] = np.min(dists)
        kcenters_df['max_centroid_dist'] = np.max(dists)
        kcenters_df['mean_centroid_dist'] = np.mean(dists)
        # add sse score
        wss = model.computeCost(transformed_data)
        kcenters_df['wss'] = wss
        # add silhouette score
        silh_evaluator = ClusteringEvaluator().setFeaturesCol("features") \
                                              .setPredictionCol(pred_col) \
                                              .setMetricName("silhouette")
        silscore = silh_evaluator.evaluate(transformed_data)
        kcenters_df['silscore'] = silscore

        return kcenters_df

    def evaluate_clusters(self, k_start, k_end, data, estimators):
        """
        Evaluate Kmeans for various algorithms and K

        PARAMETERS:
            k_start - lower bound k
            k_end - upper bound k
            data - customer data along with genres and habits
            estimators (list) - clustering estimators

        RETURNS: 
            cluster_scores (Pandas df) - summary statistics/diagnostics
            models_dict - saved model
            data - saved transformed data
        """
        # save data
        models_dict = {}
        cluster_scores = []
        for estimator in estimators:
            for k in range(k_start, k_end + 1):
                print("Constructing Scoring Results for Estimator=%s; K=%d \n" %(estimator, k))
                # fit and transform our model
                model, data = self.fit_kmeans(k, data, estimator)
                pred_col = '%s_%s' %(estimator.__str__().split("_")[0], k)
                models_dict[pred_col] = model
                data = data.withColumnRenamed('prediction', pred_col)

                # generate kmeans metrics by cluster
                kcenters_df = self.generate_kmeans_metrics(model, data, estimator, k, pred_col)
                
                # create percentage breakdowns by cluster for particular algo_k run
                counts_df = data.groupby(pred_col) \
                                            .count() \
                                            .withColumn("percent",
                                                    col("count") / data.count() * 100) \
                                            .orderBy("count")
                counts_df.show()
                counts_df = counts_df.toPandas()                                    
                
                # merge the 2 datasets on cluster centroid
                kcenters_df = kcenters_df.merge(counts_df,
                                                left_on = 'center_id',
                                                right_on = pred_col).drop(columns=pred_col)

                cluster_scores.append(kcenters_df)

        cluster_score = pd.concat(cluster_scores, axis=0)
        
        return cluster_score, models_dict, data



# PROBABILITY QUANTILE
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def run_segment_prob_quartile(marketplace_id, marketplace_name, start_date, end_date, spark=None, sqlContext=None):
    # 0 sync the EMRFS
    sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/SegmentsLabels'])

    if not spark:
        sc = SparkContext(appName="SegmentRefreshSummaryStats")# redundant in pyspark shell
        sqlContext = SQLContext(sc)
        spark = SparkSession.builder.getOrCreate()# redundant in pyspark shell
        # sc.addPyFile('constants.py')
        # sc.addPyFile('utils.py')

    # from constants import SEG_DIST_PROB_PATH, SEG_DIST_PROB_SCHEMA, HABIT_GENRE_UNIFY_SEG_SCHEMA, GENRE_HABITS_SCHEMA, PATH
    # from utils import gather_data

    #### 1.1 READ records of aggregated prime video eligible days for each customers
    habit_genre_unify_filepath = SEG_DIST_PROB_PATH + '%s_genre_habit_segment_labels_%s_%s' % (marketplace_name, start_date, end_date)
    cust_seg_labels = gather_data(spark, HABIT_GENRE_UNIFY_SEG_SCHEMA, habit_genre_unify_filepath, 
                                  delimiter = ',').cache()
    ## OLD WAY                                        
    #cust_seg_labels = spark.read.format("com.databricks.spark.csv")\
    #                            .option("header", "true")\
    #                            .option("inferSchema", "true")\
    #                            .option("delimiter", ",")\
    #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_habit_segment_labels_%s_%s.csv" % \
    #                                (marketplace_name, start_date, end_date))\
    #                            .cache()
    
    #cust_seg_labels.printSchema()
    # root
    #  |-- customer_id: long (nullable = true)
    #  |-- segment_habit_id: integer (nullable = true)
    #  |-- segment_genre_id: integer (nullable = true)
    #  |-- is_high_streamer: integer (nullable = true)
    #  |-- profile_begin: integer (nullable = true)
    #  |-- profile_end: integer (nullable = true)

    # compute absolute and relative size of each segment
    total_seg_cust = cust_seg_labels.count()

    df_sizePerGenreSeg = cust_seg_labels.groupBy('segment_genre_id')\
                                .agg(countDistinct('customer_id')\
                                .alias('n_customers_by_genreseg'))\
                                .orderBy('segment_genre_id')\
                                .toPandas()
    df_sizePerHabitSeg = cust_seg_labels.groupBy('segment_habit_id')\
                                .agg(countDistinct('customer_id')\
                                .alias('n_customers_by_habitseg'))\
                                .orderBy('segment_habit_id')\
                                .toPandas()

    # write the size of segment to s3
    df_sizePerGenreSeg['genre_seg_size_pct'] = df_sizePerGenreSeg['n_customers_by_genreseg'] / total_seg_cust
    bytes_to_write = df_sizePerGenreSeg.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/GenreSegSize_%s_%s_%s.csv' % (start_date, end_date, marketplace_name)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Genre Segmentation Size to %s' %temp_path)

    df_sizePerHabitSeg['habit_seg_size_pct'] = df_sizePerHabitSeg['n_customers_by_habitseg'] / total_seg_cust
    bytes_to_write = df_sizePerHabitSeg.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/HabitSegSize_%s_%s_%s.csv' % (start_date, end_date, marketplace_name)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Habit Segmentation Size to %s' %temp_path)

    #### 1.2 reading and joining with HABIT segment-probability data with customer-segment grain

    # 1.4 reading and joining with GENRE segment-probability data with customer-segment grain
    genre_seg_dist_prob_filename = SEG_DIST_PROB_PATH + '%s_genre_segment_distprob_%s_%s' % (marketplace_name, start_date, end_date)
    cust_genreseg_probs = gather_data(spark, SEG_DIST_PROB_SCHEMA, genre_seg_dist_prob_filename, 
                                      delimiter = ',').cache()
    ## OLD WAY
    #cust_genreseg_probs = spark.read.format("com.databricks.spark.csv")\
    #                            .option("header", "true")\
    #                            .option("inferSchema", "true")\
    #                            .option("delimiter", ",")\
    #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_segment_distprob_%s_%s.csv" % \
    #                                    (marketplace_name, start_date, end_date))\
    #                            .cache()
    cust_genreseg_probs = cust_genreseg_probs.select('customer_id', col("sid").alias("segment_genre_id"), col('probability').alias('genre_prob'))
    df_joined = cust_seg_labels.join(cust_genreseg_probs,['customer_id','segment_genre_id'], "left")

    habit_seg_dist_prob_filename = SEG_DIST_PROB_PATH + '%s_habit_segment_distprob_%s_%s' % (marketplace_name, start_date, end_date)
    cust_habitseg_probs = gather_data(spark, SEG_DIST_PROB_SCHEMA, habit_seg_dist_prob_filename, 
                                      delimiter = ',').cache()
    ## OLD WAY
    #cust_habitseg_probs = spark.read.format("com.databricks.spark.csv")\
    #                            .option("header", "true")\
    #                            .option("inferSchema", "true")\
    #                            .option("delimiter", ",")\
    #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_habit_segment_distprob_%s_%s.csv" % \
    #                                    (marketplace_name, start_date, end_date))\
    #                            .cache()
    cust_habitseg_probs = cust_habitseg_probs.select('customer_id', col("sid").alias("segment_habit_id"), col('probability').alias('habit_prob'))
    df_joined = df_joined.join(cust_habitseg_probs,['customer_id','segment_habit_id'], "left")


    # sql way
    df_joined.registerTempTable('df_joined')
    df_genre_prob_dist = sqlContext.sql("SELECT segment_genre_id, \
                                            is_high_streamer, \
                                            min(genre_prob) as min_genre_prob, \
                                            percentile_approx(genre_prob, 0.25) as lqt_genre_prob, \
                                            percentile_approx(genre_prob, 0.5) as med_genre_prob, \
                                            percentile_approx(genre_prob, 0.75) as hqt_genre_prob, \
                                            max(genre_prob) as max_genre_prob \
                                        from df_joined \
                                        group by segment_genre_id, is_high_streamer \
                                        order by segment_genre_id, is_high_streamer")

    df_habit_prob_dist = sqlContext.sql("SELECT segment_habit_id, \
                                            is_high_streamer, \
                                            min(habit_prob) as min_habit_prob, \
                                            percentile_approx(habit_prob, 0.25) as lqt_habit_prob, \
                                            percentile_approx(habit_prob, 0.5) as med_habit_prob, \
                                            percentile_approx(habit_prob, 0.75) as hqt_habit_prob, \
                                            max(habit_prob) as max_habit_prob \
                                        from df_joined \
                                        group by segment_habit_id, is_high_streamer \
                                        order by segment_habit_id, is_high_streamer")

    df_genre_prob_dist.registerTempTable('df_genre_prob_dist')
    df_habit_prob_dist.registerTempTable('df_habit_prob_dist')

    df_joined = sqlContext.sql("SELECT df_joined.customer_id, \
                                    df_joined.is_high_streamer, \
                                    df_joined.segment_genre_id, \
                                    case when genre_prob < lqt_genre_prob then 1 \
                                            when genre_prob BETWEEN lqt_genre_prob AND med_genre_prob THEN 2 \
                                            when genre_prob BETWEEN med_genre_prob AND hqt_genre_prob THEN 3 \
                                            when genre_prob > hqt_genre_prob THEN 4 \
                                            END AS genre_prototypical_quartile, \
                                    df_joined.segment_habit_id, \
                                    case when habit_prob < lqt_habit_prob then 1 \
                                            when habit_prob BETWEEN lqt_habit_prob AND med_habit_prob THEN 2 \
                                            when habit_prob BETWEEN med_habit_prob AND hqt_habit_prob THEN 3 \
                                            when habit_prob > hqt_habit_prob THEN 4 \
                                            END AS habit_prototypical_quartile \
                            FROM df_joined \
                            JOIN df_genre_prob_dist \
                                ON df_joined.segment_genre_id = df_genre_prob_dist.segment_genre_id \
                                AND df_joined.is_high_streamer = df_genre_prob_dist.is_high_streamer \
                            JOIN df_habit_prob_dist \
                                ON df_joined.segment_habit_id = df_habit_prob_dist.segment_habit_id \
                                AND df_joined.is_high_streamer = df_habit_prob_dist.is_high_streamer")

    # alternative is to use QuantileDiscretizer directly from pyspark.ml.feature package
    # the intemedicate quantile statistics could be extracted by qds.getSplits()
    # not not easy to work on group object

    # append marketplace_id and country_code manually for ETL ease
    df_joined = df_joined.withColumn('marketplace_id', lit(int(marketplace_id)))\
                        .withColumn('country_code', lit(marketplace_name.upper()))

    # write the customer-level labels and prototypical quartile rank into s3.
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s' % (end_date, marketplace_name)
    df_joined = df_joined.select('marketplace_id','country_code','customer_id','segment_genre_id','segment_habit_id','is_high_streamer','genre_prototypical_quartile','habit_prototypical_quartile')
    df_joined.write.csv(path=temp_path,
        mode='overwrite', sep=',', header=False)
    print("Writing Genre Habit Segment Labels to %s" %temp_path)

    # Update
    # First write / combine into new single file folder
    df_joined.coalesce(1).write.csv(path=temp_path + '.csv', mode='overwrite', sep=',', header=False)
    
    # rename the actual datafile and move it
    s3_resource = boto3.resource('s3')
    bucket_name = 'studiosresearch-projects'
    prefix = 'customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s.csv' % (end_date, marketplace_name)
    bucket = s3_resource.Bucket(bucket_name)
    saved_files = []
    for key in bucket.objects.filter(Prefix=prefix):
        file_name = key.key.split('/')[-1]
        if file_name.startswith('part-0'):
            saved_files.append("%s/%s" %(bucket_name, key.key))
    # double-check should only be 1 file
    assert len(saved_files)==1

    # Copy object A as object B
    s3_resource.Object(bucket_name, prefix).copy_from(CopySource=saved_files[0])
    # Finally delete that folder
    bucket.objects.filter(Prefix="%s/" %prefix).delete()
    print("Writing Single CSV - Genre Habit Segment Labels to %s" %prefix)

    # 1.2 read the raw streaming record to derive
    ## % active streaming days
    ## number of titlesk
    ## tv/movie split
    ## svod/tvod split
    ## persegment genre% average profile
    # s3_path = "%s%s_%s-%s-%s/" %(PATH, marketplace_id, start_date, end_date, marketplace_name.upper())
    s3_path = "%s%s_%s_%s/" %(PATH, marketplace_id, marketplace_name.upper(), end_date)
    cust_streams = gather_data(spark, GENRE_HABITS_SCHEMA, s3_path).cache()
    
    ## OLD WAY
    #cust_streams = spark.read.format("com.databricks.spark.csv")\
    #                        .option("header", "false")\
    #                        .option("inferSchema", "false")\
    #                        .option("delimiter", "\t")\
    #                        .load("s3://studiosresearch-projects/customerSegmentation/redshift-unloads/CUSTOMER_GENRE_HABIT_LANG_PROFILE_MKT%s_%s-%s-%s/" % \
    #                            (marketplace_id, start_date, end_date, marketplace_name.upper()), schema=GENRE_HABITS_SCHEMA)\
    #                        .cache()

    # 1.3 Left join the segment ID data with streaming data
    df_joined = df_joined.join(cust_streams, 'customer_id', 'left')

    # compute the denominator of "activity level", i.e. days since first stream in profile window
    # end_date_str = '%s-%s-%s 00:00:00' % (end_date[:4], end_date[4:6], end_date[6:])
    # df_joined = df_joined.withColumn('days_since_first_stream', datediff(lit(end_date_str), col('first_stream_date')))
    df_joined = df_joined.withColumn('%active_days', col('distinct_streaming_days')/col('days_since_first_stream'))

    # aggregate various metrics grouped by genre_segment_id
    metrics_of_time = [
        'minutes_weekday_sleep', 'minutes_weekday_morning', 'minutes_weekday_afternoon',
        'minutes_weekday_night', 'minutes_weekend',
        'minutes_tv', 'minutes_movie', 'minutes_SVOD', 'minutes_TVOD',
        'minutes_living_room', 'minutes_mobile', 'minutes_web',
        'minutes_english_audio', 'minutes_french_audio', 'minutes_spanish_audio',
        'minutes_german_audio', 'minutes_japanese_audio', 'minutes_hindi_audio',
        'minutes_telugu_audio', 'minutes_tamil_audio', 'minutes_italian_audio', 'minutes_portuguese_audio', 'minutes_unknown_audio'] 

    metrics_not_of_time = [
        'titles_viewed','avg_awards', 'avg_ratings', 'freq_ratings']

    metrics_of_genre = [
        'action', 'adult', 'adventure', 'animation', 'biography', 'comedy', 'crime',
        'documentary', 'drama', 'family', 'fantasy', 'game_show', 'history', 'horror',
        'music', 'musical', 'mystery', 'news', 'reality_tv', 'romance', 'sci_fi', 'short',
        'sport', 'talk_show', 'thriller', 'war', 'western', 'caper', 'soap', 'anthology',
        'world_creation', 'african_american', 'middle_east', 'lgbt', 'female_lead', 'dystopia',
        'anime', 'legal', 'police', 'psychological', 'political', 'satire', 'disaster',
        'dark_humor', 'spy', 'superhero', 'dark', 'supernatural', 'big_city', 'based_on_lit',
        'outer_space', 'based_on_true', 'young_adults', 'period_1900s', 'period_before_1900']

    for m in metrics_of_time + metrics_of_genre:
        df_joined = df_joined.withColumn('%'+m, col(m) / col('minutes_viewed'))

    metrics_to_agg = ['minutes_viewed', 'titles_viewed','%active_days'] + ['%'+m for m in metrics_of_time + metrics_of_genre]

    # Compute mean metrics per Segment / high-low streamer / prototypical quartile grain, as well as rollup
    df_stats_by_genreseg = df_joined.select(['segment_genre_id','is_high_streamer','genre_prototypical_quartile'] + metrics_to_agg)\
            .rollup('segment_genre_id','is_high_streamer','genre_prototypical_quartile')\
            .mean()\
            .orderBy('segment_genre_id','is_high_streamer','genre_prototypical_quartile')\
            .toPandas()
    # write to s3
    bytes_to_write = df_stats_by_genreseg.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/SummaryStatsByGenreSeg_%s_%s_%s.csv' % (start_date, end_date, marketplace_name)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Genre Summary Stats to %s' %temp_path)

    df_stats_by_habitseg = df_joined.select(['segment_habit_id','is_high_streamer','habit_prototypical_quartile'] + metrics_to_agg)\
            .rollup('segment_habit_id','is_high_streamer','habit_prototypical_quartile')\
            .mean()\
            .orderBy('segment_habit_id','is_high_streamer','habit_prototypical_quartile')\
            .toPandas()
    bytes_to_write = df_stats_by_habitseg.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/SummaryStatsByHabitSeg_%s_%s_%s.csv' % (start_date, end_date, marketplace_name)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Habit Summary Stats to %s' %temp_path)


# TRANSITION PROBABILITY
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def run_segment_transition_prob(marketplace_name, 
                                start_date1, 
                                end_date1, 
                                start_date2, 
                                end_date2,
                                spark=None, 
                                sqlContext=None):
    # 0 sync the EMRFS
    sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/SegmentsLabels'])

    if not spark:
        sc = SparkContext(appName="Segment Trans Prob %s" % marketplace_name)# redundant in pyspark shell
        sqlContext = SQLContext(sc)
        spark = SparkSession.builder.getOrCreate()# redundant in pyspark shell
        # sc.addPyFile('constants.py')
        # sc.addPyFile('utils.py')

    # from constants import PREV_SEGLABEL_SCHEMA, CURR_SEGLABEL_SCHEMA
    # from utils import gather_data

    # 1. read the previous and current segment labels
    temp_path = "s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s" %(end_date1, marketplace_name)
    label_prev = gather_data(spark, PREV_SEGLABEL_SCHEMA, temp_path, delimiter=",")
    label_prev = label_prev.select('customer_id','segment_genre_id_prev','segment_habit_id_prev').cache()
    
    ## OLD WAY
    #label_prev = spark.read.format("com.databricks.spark.csv")\
    #                    .option("header", "false")\
    #                    .option("inferSchema", "false")\
    #                    .option("delimiter", ",")\
    #                    .load(temp_path, schema=PREV_SEGLABEL_SCHEMA)\
    #                    .select('customer_id','segment_genre_id_prev','segment_habit_id_prev')\
    #                    .cache()
    #print('Reading Data from %s' %temp_path)

    temp_path = "s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s" %(end_date2, marketplace_name)
    label_curr = gather_data(spark, CURR_SEGLABEL_SCHEMA, temp_path, delimiter=',')
    label_curr = label_curr.select('customer_id','segment_genre_id_curr','segment_habit_id_curr').cache()
    
    ## OLD WAY
    #label_curr = spark.read.format("com.databricks.spark.csv")\
    #                    .option("header", "false")\
    #                    .option("inferSchema", "false")\
    #                    .option("delimiter", ",")\
    #                    .load(temp_path, schema=CURR_SEGLABEL_SCHEMA)\
    #                    .select('customer_id','segment_genre_id_curr','segment_habit_id_curr')\
    #                    .cache()
    #print('Reading Data from %s' %temp_path)

    # Join the previous and current segment labels on customer_id
    df_joined = label_prev.join(label_curr,'customer_id', "outer")

    # Genre transition calculation
    genre_trans_mat_grped = df_joined.groupby('segment_genre_id_prev', 'segment_genre_id_curr')\
                            .agg(countDistinct('customer_id').alias('ncust')).toPandas()
    # pivot the grouped count to prev vs curr seg size
    # na on prev means newly joined customers
    # na on curr means churned customers
    genre_trans_mat_count = genre_trans_mat_grped.pivot(index='segment_genre_id_prev',
                                columns='segment_genre_id_curr',
                                values='ncust')
    # convert the raw count to percentage by dividing rowsum (total in each prev segment)
    genre_trans_mat_pct = genre_trans_mat_count.div(genre_trans_mat_count.sum(axis=1), axis='rows')

    # optionally write the transition matrix (pct) into s3
    bytes_to_write = genre_trans_mat_pct.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/GenreSegTransitPct_%s_%s_%s_to_%s_%s.csv' %(marketplace_name,start_date1,end_date1,start_date2, end_date2)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Genre Transition Probabilities to %s' %temp_path)

    # Habit transition calculation
    habit_trans_mat_grped = df_joined.groupby('segment_habit_id_prev', 'segment_habit_id_curr')\
                            .agg(countDistinct('customer_id').alias('ncust')).toPandas()
    # pivot the grouped count to prev vs curr seg size
    # na on prev means newly joined customers
    # na on curr means churned customers
    habit_trans_mat_count = habit_trans_mat_grped.pivot(index='segment_habit_id_prev',
                                columns='segment_habit_id_curr',
                                values='ncust')
    # convert the raw count to percentage by dividing rowsum (total in each prev segment)
    habit_trans_mat_pct = habit_trans_mat_count.div(habit_trans_mat_count.sum(axis=1), axis='rows')

    # optionally write the transition matrix (pct) into s3
    bytes_to_write = habit_trans_mat_pct.to_csv(None).encode()
    temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/HabitSegTransitPct_%s_%s_%s_to_%s_%s.csv' %(marketplace_name,start_date1,end_date1,start_date2, end_date2)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Habit Transition Probabilities to %s' %temp_path)



# TOP TITLES
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def run_top_title_by_segment(marketplace_id, marketplace_name, start_date, end_date, spark=None, sqlContext=None):
    sub.call(['emrfs','sync','s3://research-tmp-test/'])
    sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s/' % (end_date, marketplace_name)])

    if not spark:
        sc = SparkContext(appName="Top Titles")# redundant in pyspark shell
        sqlContext = SQLContext(sc)
        spark = SparkSession.builder.getOrCreate()# redundant in pyspark shell
        # sc.addPyFile('constants.py')
        # sc.addPyFile('utils.py')

    # from constants import STREAM_SCHEMA_TITLES, SEGMENT_SCHEMA
    # from utils import gather_data

    # temp_path = "s3://research-tmp-test/CUST_TITLE_AGGTIME_MKT%s_%s-%s-%s" % (marketplace_id, 
    # start_date, end_date, marketplace_name.upper())
    temp_path = "s3://research-tmp-test/CUST_TITLE_AGGTIME_MKT%s_%s_%s" % (marketplace_id, marketplace_name.upper(), end_date)
    cust_streams = gather_data(spark, STREAM_SCHEMA_TITLES, temp_path).cache()
    
    ## OLD WAY
    #cust_streams = spark.read.format("com.databricks.spark.csv")\
    #                        .option("header", "false")\
    #                        .option("inferSchema", "false")\
    #                        .option("delimiter", "\t")\
    #                        .load(temp_path, schema=STREAM_SCHEMA_TITLES)\
    #                        .cache()
    #print('Reading Data from %s' %temp_path)    
    
    temp_path = "s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/GenreHabitSegmentLables_%s_%s/" %(end_date, marketplace_name)
    cust_segment = gather_data(spark, SEGMENT_SCHEMA, temp_path, delimiter=",").cache()

    ## OLD WAY
    #cust_segment = spark.read.format("com.databricks.spark.csv")\
    #                        .option("header", "false")\
    #                        .option("inferSchema", "false")\
    #                        .option("delimiter", ",")\
    #                        .load(temp_path, schema=SEGMENT_SCHEMA)\
    #                        .cache()
    #print('Reading Data from %s' %temp_path)

    # check the overlap (coverage of customers) of the two
    stream_cid = cust_streams.select(col('customer_id').alias('stream_cid')).distinct()
    seg_cid = cust_segment.select(col('customer_id').alias('seg_cid'))
    dfcov = stream_cid.join(seg_cid, (stream_cid.stream_cid==seg_cid.seg_cid), "outer")
    dfcov.filter(col('seg_cid').isNull()).select('stream_cid').distinct().count()
    dfcov.filter(col('stream_cid').isNotNull()).filter(col('seg_cid').isNotNull()).distinct().count()


    df_joined = cust_streams\
        .join(cust_segment, 'customer_id', 'inner')
    df_joined.registerTempTable('df_joined')

    toptitle = sqlContext.sql(
        "SELECT \
        ROW_NUMBER() OVER (\
            PARTITION BY segment_genre_id, content_type \
            ORDER BY hours_viewed DESC) AS rank_by_hours, \
        ROW_NUMBER() OVER (\
            PARTITION BY segment_genre_id, content_type \
            ORDER BY customers_viewed DESC) AS rank_by_customers, \
        seg_stats.*\
        FROM (\
            SELECT \
                segment_genre_id, \
                content_type,\
                tconst, \
                title, \
                gti, \
                sum(seconds_viewed) / 3600.0 AS hours_viewed, \
                count(distinct customer_id) AS customers_viewed \
            FROM df_joined \
            GROUP BY \
                segment_genre_id, \
                content_type,\
                tconst, \
                title, \
                gti \
        ) seg_stats \
        WHERE seg_stats.customers_viewed > 1000 \
        ")

    df_titlebySeg = toptitle.toPandas()
    df_titlebySeg = df_titlebySeg.query('rank_by_hours <= 100 or rank_by_customers <= 100')
    bytes_to_write = df_titlebySeg.to_csv(None, encoding = 'utf-8')
    temp_path = 's3://research-tmp-test/%s_top_titles_%s_%s.csv' % (marketplace_name, start_date, end_date)
    with s3.open(temp_path, 'wb') as f:
        f.write(bytes_to_write)
    print('Writing Top Titles to %s' %temp_path)  



# APP STATS
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def run_app_stats(marketplace_id, marketplace_name, start_date, end_date, spark=None, sqlContext=None):
  sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/SegmentsLabels'])

  if not spark:
        sc = SparkContext(appName="SegmentAppStats")# redundant in pyspark shell
        sqlContext = SQLContext(sc)
        spark = SparkSession.builder.getOrCreate()# redundant in pyspark shell
        # sc.addPyFile('constants.py')
        # sc.addPyFile('utils.py')

  # from constants import SEG_DIST_PROB_PATH, SEG_DIST_PROB_SCHEMA, HABIT_GENRE_UNIFY_SEG_SCHEMA, GENRE_HABITS_SCHEMA, PATH
  # from utils import gather_data

  #### 1.1 READ records of aggregated prime video eligible days for each customers
  habit_genre_unify_filepath = SEG_DIST_PROB_PATH + '%s_genre_habit_segment_labels_%s_%s' % (marketplace_name, start_date, end_date)
  cust_seg_labels = gather_data(spark, HABIT_GENRE_UNIFY_SEG_SCHEMA, habit_genre_unify_filepath, 
                                  delimiter = ',').cache()
  ## OLD WAY
  #cust_seg_labels = spark.read.format("com.databricks.spark.csv")\
  #                            .option("header", "true")\
  #                            .option("inferSchema", "true")\
  #                            .option("delimiter", ",")\
  #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_habit_segment_labels_%s_%s.csv" % \
  #                                (marketplace_name, start_date, end_date))\
  #                            .cache()  

  #cust_seg_labels.printSchema()
  # root
  #  |-- customer_id: long (nullable = true)
  #  |-- segment_habit_id: integer (nullable = true)
  #  |-- segment_genre_id: integer (nullable = true)
  #  |-- is_high_streamer: integer (nullable = true)
  #  |-- profile_begin: integer (nullable = true)
  #  |-- profile_end: integer (nullable = true)

  # compute absolute and relative size of each segment
  total_seg_cust = cust_seg_labels.count()

  df_sizePerGenreSeg = cust_seg_labels.groupBy('segment_genre_id')\
                                .agg(countDistinct('customer_id')\
                                .alias('n_customers_by_genreseg'))\
                                .orderBy('segment_genre_id')\
                                .toPandas()
  df_sizePerHabitSeg = cust_seg_labels.groupBy('segment_habit_id')\
                                .agg(countDistinct('customer_id')\
                                .alias('n_customers_by_habitseg'))\
                                .orderBy('segment_habit_id')\
                                .toPandas()

  # write the size of segment to s3
  df_sizePerGenreSeg['genre_seg_size_pct'] = df_sizePerGenreSeg['n_customers_by_genreseg'] / total_seg_cust
  df_sizePerHabitSeg['habit_seg_size_pct'] = df_sizePerHabitSeg['n_customers_by_habitseg'] / total_seg_cust


  #### 1.2 reading and joining with HABIT segment-probability data with customer-segment grain
  # 1.4 reading and joining with GENRE segment-probability data with customer-segment grain
  genre_seg_dist_prob_filename = SEG_DIST_PROB_PATH + '%s_genre_segment_distprob_%s_%s' % (marketplace_name, start_date, end_date)
  cust_genreseg_probs = gather_data(spark, SEG_DIST_PROB_SCHEMA, genre_seg_dist_prob_filename, 
                                    delimiter = ',').cache()
  ## OLD WAY                                  
  #cust_genreseg_probs = spark.read.format("com.databricks.spark.csv")\
  #                            .option("header", "true")\
  #                            .option("inferSchema", "true")\
  #                            .option("delimiter", ",")\
  #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_segment_distprob_%s_%s.csv" % \
  #                                    (marketplace_name, start_date, end_date))\
  #                            .cache()    

  cust_genreseg_probs = cust_genreseg_probs.select('customer_id', col("sid").alias("segment_genre_id"), col('probability').alias('genre_prob'))
  df_joined = cust_seg_labels.join(cust_genreseg_probs,['customer_id','segment_genre_id'], "left")

  habit_seg_dist_prob_filename = SEG_DIST_PROB_PATH + '%s_habit_segment_distprob_%s_%s' % (marketplace_name, start_date, end_date)
  cust_habitseg_probs = gather_data(spark, SEG_DIST_PROB_SCHEMA, habit_seg_dist_prob_filename, 
                                    delimiter = ',').cache()
  ## OLD WAY
  #cust_habitseg_probs = spark.read.format("com.databricks.spark.csv")\
  #                            .option("header", "true")\
  #                            .option("inferSchema", "true")\
  #                            .option("delimiter", ",")\
  #                            .load("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_habit_segment_distprob_%s_%s.csv" % \
  #                                    (marketplace_name, start_date, end_date))\
  #                            .cache()    
  cust_habitseg_probs = cust_habitseg_probs.select('customer_id', col("sid").alias("segment_habit_id"), col('probability').alias('habit_prob'))
  df_joined = df_joined.join(cust_habitseg_probs,['customer_id','segment_habit_id'], "left")


  # sql way
  # alternative is to use QuantileDiscretizer directly from pyspark.ml.feature package
  # the intemedicate quantile statistics could be extracted by qds.getSplits()
  # not not easy to work on group object

  # append marketplace_id and country_code manually for ETL ease
  df_joined = df_joined.withColumn('marketplace_id', lit(int(marketplace_id)))\
                      .withColumn('country_code', lit(marketplace_name.upper()))

  # write the customer-level labels and prototypical quartile rank into s3.

  # 1.2 read the raw streaming record to derive
  ## % active streaming days
  ## number of titlesk
  ## tv/movie split
  ## svod/tvod split
  ## persegment genre% average profile
  # s3_path = "%s%s_%s-%s-%s/" %(PATH, marketplace_id, start_date, end_date, marketplace_name.upper())
  s3_path = "%s%s_%s_%s/" %(PATH, marketplace_id, marketplace_name.upper(), end_date)
  cust_streams = gather_data(spark, GENRE_HABITS_SCHEMA, s3_path).cache()
    
  ## OLD WAY
  #cust_streams = spark.read.format("com.databricks.spark.csv")\
  #                        .option("header", "false")\
  #                        .option("inferSchema", "false")\
  #                        .option("delimiter", "\t")\
  #                        .load("s3://studiosresearch-projects/customerSegmentation/redshift-unloads/CUSTOMER_GENRE_HABIT_LANG_PROFILE_MKT%s_%s-%s-%s/" % \
  #                            (marketplace_id, start_date, end_date, marketplace_name.upper()), schema=GENRE_HABITS_SCHEMA)\
  #                        .cache()

  # 1.3 Left join the segment ID data with streaming data
  df_joined = df_joined.join(cust_streams, 'customer_id', 'left')

  # compute the denominator of "activity level", i.e. days since first stream in profile window
  # end_date_str = '%s-%s-%s 00:00:00' % (end_date[:4], end_date[4:6], end_date[6:])
  # df_joined = df_joined.withColumn('days_since_first_stream', datediff(lit(end_date_str), col('first_stream_date')))
  df_joined = df_joined.withColumn('%active_days', col('distinct_streaming_days')/col('days_since_first_stream'))

  # aggregate various metrics grouped by genre_segment_id
  metrics_of_time = [
      'minutes_weekday_night', 'minutes_weekend',
      'minutes_tv', 'minutes_movie', 'minutes_SVOD', 'minutes_TVOD'] 

  metrics_not_of_time = [
      'titles_viewed']

  for m in metrics_of_time:
      df_joined = df_joined.withColumn('%'+m, col(m) / col('minutes_viewed'))

  metrics_to_agg = ['minutes_viewed', 'titles_viewed','%active_days'] + ['%'+m for m in metrics_of_time]

  # Compute mean metrics per Segment / high-low streamer / prototypical quartile grain, as well as rollup
  df_stats_by_genreseg =  df_joined.select(['segment_genre_id'] + metrics_to_agg).rollup('segment_genre_id').mean().orderBy('segment_genre_id').toPandas()

  df_mean_final = pd.melt(df_stats_by_genreseg, id_vars=['segment_genre_id'], value_name="Value", var_name="Category")
  df_mean_final = df_mean_final.rename(columns = {"segment_genre_id":"Segment"}).assign(Country = marketplace_name.upper(),Statistic = "Mean", Date = end_date)
  df_mean_final['Category'] = df_mean_final['Category'].map(lambda x: x.lstrip("avg(").rstrip(")"))
  df_mean_final = df_mean_final.query("Category != 'segment_genre_id'").fillna(value = 'Overall')
  df_mean_final['Segment'] = df_mean_final['Segment'].astype(str)

  # Grab Centroids to use the segment labels
  segment_names = gather_data(spark, schema=None, s3_path="s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentNames/dim_segmentation_genre_%s.csv" %marketplace_name.lower(), delimiter=',').toPandas()
  # need cluster_id to be object to be able to join 
  segment_names.segment_genre_id = segment_names.segment_genre_id.map(lambda x: str(x))

  # Join back to df and overwrite the segment id with segment human readable label
  df_mean_final = pd.merge(df_mean_final, 
                             segment_names[['segment_genre_id','segment_easy_name']], 
                             left_on=['Segment'],
                             right_on=['segment_genre_id'],
                             how="left")
  df_mean_final['Segment'] = df_mean_final.segment_easy_name.combine_first(df_mean_final.Segment)
  df_mean_final.drop(['segment_genre_id','segment_easy_name'], axis=1, inplace=True)
       
  # if marketplace_name == 'us':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("101", "World Creation Action").replace("102", "Family Animation Adventure").replace("103", "Cops Crime and Caper").replace("104", "Lit Based Soap").replace("105", "Preschool Animation").replace("107", "Comedy Reality and Game").replace("108", "Big City Relationship Drama Comedy").replace("109", "Supernatural Horror Mystery"))
  # elif marketplace_name == 'gb':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("301", "Comedy Talkshow Documentary").replace("302", "Action Adventure Sci-fi").replace("303", "Big City Romance Comedy").replace("304", "Supernatural Horror Thriller").replace("305", "Lit-Based Romance History War").replace("306", "Police Crime Thriller").replace("307", "Family Animation Adventure"))
  # elif marketplace_name == 'de':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("401", "Science Fiction Action Adventure World Creation").replace("402", "Police Crime Thriller").replace("403", "Supernatural Horror Fantasy Mystery").replace("404", "Urban Comedy Satire").replace("405", "Romance Soap History").replace("406", "Family Animation Adventure"))
  # elif marketplace_name == 'jp':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("601", "Anime and Animation").replace("602", "Big City Romance Soap").replace("603", "Family Animation").replace("604", "World Creation").replace("605", "Crime Police Thriller").replace("606", "Popular Shows and Reality TV"))
  # elif marketplace_name == 'it':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("901", "Supernatural").replace("902", "Action Adventure").replace("903", "Dystopian Science Fiction and Spy").replace("904", "Romance").replace("905", "Sports Talk Show and Comedy").replace("906", "Crime Police Thriller").replace("907", "Comedy").replace("908", "Family Animation")) 
  # elif marketplace_name == 'es':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("1001", "Family Animation").replace("1002", "Sports Talk Show and Comedy").replace("1003", "Supernatural").replace("1004", "Action Adventure").replace("1005", "Dystopian Science Fiction and Spy").replace("1006", "Romance").replace("1007", "Crime Police Thriller").replace("1008", "Comedy"))
  # elif marketplace_name == 'fr':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("1101", "Supernatural").replace("1102", "Sports Talk Show and Comedy").replace("1103", "Action Adventure").replace("1104", "Family Animation").replace("1105", "Crime Police Thriller").replace("1106", "Romance").replace("1107", "Dystopian Science Fiction and Spy"))
  # elif marketplace_name == 'ca':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("1201", "Drama & Biography").replace("1202", "Action & Adventure").replace("1203", "Police, Crime & Drama").replace("1204", "Romantic Comedy").replace("1205", "Reality TV, Talk Shows & Sports").replace("1206", "Sci-Fi, Spy & Dystopia").replace("1207", "Satire & Politics").replace("1208", "Family & Animation").replace("1209", "Fantasy, Horror & Supernatural"))
  # elif marketplace_name == 'mx':
  #   df_mean_final['Segment'] = df_mean_final['Segment'].map(lambda x: x.replace("1301", "Comedy Reality and Game").replace("1302", "Big City Relationship Comedy").replace("1303", "Family Animation Adventure").replace("1304", "Realistic Drama").replace("1305", "Cops Crime and Caper").replace("1306", "Supernatural Horror Fantasy").replace("1307", "Scifi Action"))

  df_mean_final['Category'] = df_mean_final['Category'].map(lambda x: x.replace("minutes_viewed", "Minutes").replace("titles_viewed", "Titles").replace("%active_days", "% Active Days").replace("%minutes_weekday_night", "% Minutes Weekday Night").replace("%minutes_weekend", "% Minutes Weekend").replace("%minutes_tv", "% Minutes TV").replace("%minutes_movie", "% Minutes Movie").replace("%minutes_SVOD","% Minutes SVOD").replace("%minutes_TVOD", "% Minutes TVOD"))
  df_mean_final = df_mean_final[["Country", "Statistic", "Category", "Date", "Value", "Segment"]]
  # write to s3
  # bytes_to_write = df_mean_final.to_csv(None, index = False).encode()
  # with s3.open('s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/App_Stats_Segmentation_Mean_%s_%s_%s.csv' % (start_date, end_date, marketplace_name), 'wb') as f:
  #     f.write(bytes_to_write)


  df_sql = df_joined.select(['segment_genre_id'] + metrics_to_agg)
  df_sql.registerTempTable('df_sql')

  df_med_seg = sqlContext.sql("SELECT segment_genre_id,\
                                  percentile_approx(minutes_viewed, 0.5) AS minutes_viewed,\
                                  percentile_approx(titles_viewed, 0.5) AS titles_viewed,\
                                  percentile_approx(`%minutes_TVOD`, 0.5) AS minutes_TVOD, \
                                  percentile_approx(`%minutes_SVOD`, 0.5) AS minutes_SVOD, \
                                  percentile_approx(`%active_days`, 0.5) AS active_days, \
                                  percentile_approx(`%minutes_weekday_night`, 0.5) AS minutes_weekday_night, \
                                  percentile_approx(`%minutes_weekend`, 0.5) AS minutes_weekend, \
                                  percentile_approx(`%minutes_tv`, 0.5) AS minutes_tv, \
                                  percentile_approx(`%minutes_movie`, 0.5) AS minutes_movie \
                                      from df_sql \
                                      GROUP BY segment_genre_id \
                                      ORDER BY segment_genre_id") 

  df_med_overall = sqlContext.sql("SELECT \
                                  percentile_approx(minutes_viewed, 0.5) AS minutes_viewed,\
                                  percentile_approx(titles_viewed, 0.5) AS titles_viewed,\
                                  percentile_approx(`%minutes_TVOD`, 0.5) AS minutes_TVOD, \
                                  percentile_approx(`%minutes_SVOD`, 0.5) AS minutes_SVOD, \
                                  percentile_approx(`%active_days`, 0.5) AS active_days, \
                                  percentile_approx(`%minutes_weekday_night`, 0.5) AS minutes_weekday_night, \
                                  percentile_approx(`%minutes_weekend`, 0.5) AS minutes_weekend, \
                                  percentile_approx(`%minutes_tv`, 0.5) AS minutes_tv, \
                                  percentile_approx(`%minutes_movie`, 0.5) AS minutes_movie \
                                      from df_sql") 
                                      
  df_med_overall = df_med_overall.toPandas()
  df_med_seg = df_med_seg.toPandas()   

  df_med_overall['segment_genre_id'] = 'Overall'
  df_med_overall = df_med_overall[['segment_genre_id'] + [i for i in list(df_med_overall.columns.values) if i != 'segment_genre_id']]
  df_med_overall = pd.concat([df_med_overall,df_med_seg], sort = False) 

  df_median_final = pd.melt(df_med_overall, id_vars=['segment_genre_id'], value_name="Value", var_name="Category")
  df_median_final = df_median_final.rename(columns = {"segment_genre_id":"Segment"}).assign(Country = marketplace_name.upper(), Statistic = "Median", Date = end_date)
  df_median_final['Segment'] = df_median_final['Segment'].astype(str)

  # Grab Centroids to use the segment labels
  segment_names = gather_data(spark, schema=None, s3_path="s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentNames/dim_segmentation_genre_%s.csv" %marketplace_name.lower(), delimiter=',').toPandas()
  # need cluster_id to be object to be able to join 
  segment_names.segment_genre_id = segment_names.segment_genre_id.map(lambda x: str(x))

  # Join back to df and overwrite the segment id with segment human readable label
  df_median_final = pd.merge(df_median_final, 
                              segment_names[['segment_genre_id','segment_easy_name']], 
                              left_on=['Segment'],
                              right_on=['segment_genre_id'],
                              how="left")
  df_median_final['Segment'] = df_median_final.segment_easy_name.combine_first(df_median_final.Segment)
  df_median_final.drop(['segment_genre_id','segment_easy_name'], axis=1, inplace=True)
     
  # if marketplace_name == 'us':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("101", "World Creation Action").replace("102", "Family Animation Adventure").replace("103", "Cops Crime and Caper").replace("104", "Lit Based Soap").replace("105", "Preschool Animation").replace("107", "Comedy Reality and Game").replace("108", "Big City Relationship Drama Comedy").replace("109", "Supernatural Horror Mystery"))
  # elif marketplace_name == 'gb':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("301", "Comedy Talkshow Documentary").replace("302", "Action Adventure Sci-fi").replace("303", "Big City Romance Comedy").replace("304", "Supernatural Horror Thriller").replace("305", "Lit-Based Romance History War").replace("306", "Police Crime Thriller").replace("307", "Family Animation Adventure"))  
  # elif marketplace_name == 'de':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("401", "Science Fiction Action Adventure World Creation").replace("402", "Police Crime Thriller").replace("403", "Supernatural Horror Fantasy Mystery").replace("404", "Urban Comedy Satire").replace("405", "Romance Soap History").replace("406", "Family Animation Adventure"))
  # elif marketplace_name == 'jp':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("601", "Anime and Animation").replace("602", "Big City Romance Soap").replace("603", "Family Animation").replace("604", "World Creation").replace("605", "Crime Police Thriller").replace("606", "Popular Shows and Reality TV"))
  # elif marketplace_name == 'it':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("901", "Supernatural").replace("902", "Action Adventure").replace("903", "Dystopian Science Fiction and Spy").replace("904", "Romance").replace("905", "Sports Talk Show and Comedy").replace("906", "Crime Police Thriller").replace("907", "Comedy").replace("908", "Family Animation")) 
  # elif marketplace_name == 'es':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("1001", "Family Animation").replace("1002", "Sports Talk Show and Comedy").replace("1003", "Supernatural").replace("1004", "Action Adventure").replace("1005", "Dystopian Science Fiction and Spy").replace("1006", "Romance").replace("1007", "Crime Police Thriller").replace("1008", "Comedy")) 
  # elif marketplace_name == 'fr':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("1101", "Supernatural").replace("1102", "Sports Talk Show and Comedy").replace("1103", "Action Adventure").replace("1104", "Family Animation").replace("1105", "Crime Police Thriller").replace("1106", "Romance").replace("1107", "Dystopian Science Fiction and Spy"))
  # elif marketplace_name == 'ca':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("1201", "Drama & Biography").replace("1202", "Action & Adventure").replace("1203", "Police, Crime & Drama").replace("1204", "Romantic Comedy").replace("1205", "Reality TV, Talk Shows & Sports").replace("1206", "Sci-Fi, Spy & Dystopia").replace("1207", "Satire & Politics").replace("1208", "Family & Animation").replace("1209", "Fantasy, Horror & Supernatural"))
  # elif marketplace_name == 'mx':
  #   df_median_final['Segment'] = df_median_final['Segment'].map(lambda x: x.replace("1301", "Comedy Reality and Game").replace("1302", "Big City Relationship Comedy").replace("1303", "Family Animation Adventure").replace("1304", "Realistic Drama").replace("1305", "Cops Crime and Caper").replace("1306", "Supernatural Horror Fantasy").replace("1307", "Scifi Action"))


  df_median_final['Category'] = df_median_final['Category'].map(lambda x: x.replace("minutes_viewed", "Minutes").replace("titles_viewed", "Titles").replace("active_days", "% Active Days").replace("minutes_weekday_night", "% Minutes Weekday Night").replace("minutes_weekend", "% Minutes Weekend").replace("minutes_tv", "% Minutes TV").replace("minutes_movie", "% Minutes Movie").replace("minutes_SVOD","% Minutes SVOD").replace("minutes_TVOD", "% Minutes TVOD"))
  df_median_final = df_median_final[["Country", "Statistic", "Category", "Date", "Value", "Segment"]]                                    

  df_overall = pd.concat([df_median_final, df_mean_final])
  bytes_to_write = df_overall.to_csv(None, index = False).encode()
  temp_path = 's3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SummaryStats/App_Stats_Segmentation_Overall_%s_%s_%s.csv' % (start_date, end_date, marketplace_name)
  with s3.open(temp_path, 'wb') as f:
      f.write(bytes_to_write)                                      
  print("Writing App Stats to %s" %temp_path)





# MAIN
###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################

def run_segmentation(end_date_str, 
                     monthly_refresh_rate,
                     yearly_cadence,
                     marketplace_id,
                     marketplace_name,
                     spark=None, 
                     sqlContext=None):
    """
    Main Function to run Customer Segmenation Refresh
    Also contains uncommented code for running Kmeans
    """
    #sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/redshift-unloads'])
    sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/centroids'])
 
    # Initialize date comparisons
    start_date = pd.to_datetime(end_date_str) - dateutil.relativedelta.relativedelta(years=yearly_cadence) + dateutil.relativedelta.relativedelta(days=1)
    start_date_str = start_date.strftime('%Y%m%d')

    end_date_str_2 = pd.to_datetime(end_date_str) - dateutil.relativedelta.relativedelta(months=monthly_refresh_rate) + dateutil.relativedelta.relativedelta(day=31)
    start_date_str_2 = end_date_str_2 - dateutil.relativedelta.relativedelta(years=yearly_cadence) + dateutil.relativedelta.relativedelta(days=1)
    start_date_str_2 = start_date_str_2.strftime('%Y%m%d')
    end_date_str_2 = end_date_str_2.strftime('%Y%m%d')
    print("Running segmentation between %s and %s for %s-%s" %(start_date_str, end_date_str, marketplace_name, marketplace_id))
    print("Comparing with segments between %s and %s" %(start_date_str_2, end_date_str_2))

    ###############################################
    ###### INITIALIZE SPARK AND LIBRARIES #########
    ###############################################
    if not spark:
        #myconf = make_emr_config()
        #conf = SparkConf().setAll(myconf)
        sc = SparkContext(appName="CustomerSegmentationMain")# redundant in pyspark shell
        sqlContext = SQLContext(sc)
        spark = SparkSession.builder.getOrCreate()# redundant in pyspark shell
        # sc.addPyFile('constants.py')
        # sc.addPyFile('utils.py')
        # sc.addPyFile('segment.py')
        # sc.addPyFile('run_prob_quartile.py')
        # sc.addPyFile('run_transition_prob.py')
        # sc.addPyFile('run_top_titles.py')
        # sc.addPyFile('run_app_stats_segmentation.py')

    # from constants import GENRES, HABITS, GENRE_HABITS_SCHEMA, PATH, SEG_DIST_PROB_PATH, SEG_DIST_PROB_SCHEMA, HABIT_GENRE_UNIFY_SEG_SCHEMA, CENTROID_PATH
    # from utils import gather_data, generate_cluster_distance_prob, unionAll
    # from segment import Segmentation
    # from run_prob_quartile import run_segment_prob_quartile
    # from run_transition_prob import run_segment_transition_prob
    # from run_top_titles import run_top_title_by_segment
    # from run_app_stats_segmentation import run_app_stats

    # Initalize paths
    schema = GENRE_HABITS_SCHEMA
    path = PATH
    # s3_path = "%s%s_%s-%s-%s/" %(path, marketplace_id, start_date_str, end_date_str, marketplace_name)
    s3_path = "%s%s_%s_%s/" %(path, marketplace_id, marketplace_name, end_date_str)


    #################################
    ### GATHER DATA FROM S3 #########
    #################################
    df = gather_data(spark, schema, s3_path)
    df.cache()


    #################################
    ###### TRANSFORMING DATA ########
    #################################
    
    ## Genres
    print('\nTransforming Genre Data')
    myseg_genres = Segmentation(df, GENRES)
    myseg_genres.transform_data()
    ## Use full dataset only for REFRESH
    full_data_genres = unionAll(myseg_genres.high_data, myseg_genres.low_data)
    full_data_genres = myseg_genres.add_modeling_feature(full_data_genres)
    full_data_genres.cache()
    ## Use high only for Training
    high_data_genres = myseg_genres.high_data
    high_data_genres = myseg_genres.add_modeling_feature(high_data_genres)
    high_data_genres.cache()

    ## Habits
    print('Transforming Habits Data')
    myseg_habits = Segmentation(df, HABITS)
    myseg_habits.transform_data()
    ## Use full dataset only for REFRESH
    full_data_habits = unionAll(myseg_habits.high_data, myseg_habits.low_data)
    full_data_habits = myseg_habits.add_modeling_feature(full_data_habits)
    full_data_habits.cache()
    ## Use high only for Training
    high_data_habits = myseg_habits.high_data
    high_data_habits = myseg_habits.add_modeling_feature(high_data_habits)
    high_data_habits.cache()


    ####################################
    ######### PULLING CENTROIDS ########
    ####################################

    print('\nPulling Existing Centroids from S3')
    temp_genres = gather_data(spark, schema=None, s3_path=CENTROID_PATH + "%s_genre_segment_centroids.csv" %marketplace_name.lower(), delimiter=',')
    temp_habits = gather_data(spark, schema=None, s3_path=CENTROID_PATH + "%s_habit_segment_centroids.csv" %marketplace_name.lower(), delimiter=',')
    cluster_centers_genres = temp_genres.select(GENRES).collect() # only want features
    cluster_centers_habits = temp_habits.select(HABITS).collect()
    
    cluster_ids_genres = temp_genres.select('cluster_id').toPandas()['cluster_id'].values
    cluster_ids_habits = temp_habits.select('cluster_id').toPandas()['cluster_id'].values

    ###################################################
    ######### COMPUTE DISTANCE & PROBABILITIES ########
    ###################################################

    ## Genres
    print('\nComputing Distances and Probabilities for Genres')
    genre_seg, \
    genre_seg_dist_prob = generate_cluster_distance_prob(full_data_genres, 
                                                         cluster_centers_genres, 
                                                         cluster_ids_genres,
                                                         euclidean, 
                                                         'inverse')
    # Habits
    print('Computing Distances and Probabilities for Habits')
    habit_seg, \
    habit_seg_dist_prob = generate_cluster_distance_prob(full_data_habits, 
                                                         cluster_centers_habits, 
                                                         cluster_ids_habits,
                                                         euclidean, 
                                                         'inverse')


    #############################################
    ### WRITING GENRES & HABITS PROBS INTO S3 ###
    #############################################
    
    # Unify Genre and Habit Segments
    habit_seg.cache()
    genre_seg.cache()

    print("\nJoining Genre and Habit Segments")
    habit_genre_unify_seg = habit_seg.join(genre_seg, ['customer_id']) \
                                     .select(habit_seg['customer_id'], 
                                             habit_seg['pred_no1'].alias('segment_habit_id'), 
                                             genre_seg['pred_no1'].alias('segment_genre_id'), 
                                             genre_seg['is_high_streamer'])
    habit_genre_unify_seg = habit_genre_unify_seg.withColumn('profile_begin', lit(start_date_str))
    habit_genre_unify_seg = habit_genre_unify_seg.withColumn('profile_end', lit(end_date_str))

    # Register SQL tables for easier submission to S3
    genre_seg_dist_prob.registerTempTable('df_genre_prob_dist')
    habit_seg_dist_prob.registerTempTable('df_habit_prob_dist')
    habit_genre_unify_seg.registerTempTable('genre_habit_segment_labels')

    # Resync with s3 in case we deleted folders
    sub.call(['emrfs','sync','s3://studiosresearch-projects/customerSegmentation/SegmentsLabels'])

    ## Writes csv file into s3 at customer-level
    # OLD way of writing, by first collecting into single node, significantly slower
    #bytes_to_write = genre_seg_dist_prob.toPandas().to_csv(None).encode()
    #with s3.open("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_segment_distprob_%s_%s" % (marketplace_name, start_date_str, end_date_str), 'wb') as f:
    #    f.write(bytes_to_write)
    #bytes_to_write = habit_seg_dist_prob.toPandas().to_csv(None).encode()
    #with s3.open("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_habit_segment_distprob_%s_%s" % (marketplace_name, start_date_str, end_date_str), 'wb') as f:
    #    f.write(bytes_to_write)
    #bytes_to_write = habit_genre_unify_seg.toPandas().to_csv(None).encode()
    #with s3.open("s3://studiosresearch-projects/customerSegmentation/SegmentsLabels/SegmentProbability/%s_genre_habit_segment_labels_%s_%s" % (marketplace_name, start_date_str, end_date_str), 'wb') as f:
    #    f.write(bytes_to_write)

    # Faster option is to just write directly
    # Define file paths
    genre_seg_dist_prob_filepath = SEG_DIST_PROB_PATH + '%s_genre_segment_distprob_%s_%s' % (marketplace_name.lower(), start_date_str, end_date_str)
    habit_seg_dist_prob_filepath = SEG_DIST_PROB_PATH + '%s_habit_segment_distprob_%s_%s' % (marketplace_name.lower(), start_date_str, end_date_str)
    habit_genre_unify_filepath = SEG_DIST_PROB_PATH + '%s_genre_habit_segment_labels_%s_%s' % (marketplace_name.lower(), start_date_str, end_date_str)
    # Writes a folder with shards into s3 at customer-level
    print('\nWriting Distances and Probabilities into S3')
    genre_seg_dist_prob.write.csv(path = genre_seg_dist_prob_filepath, mode='overwrite', sep=',', header=False)
    habit_seg_dist_prob.write.csv(path = habit_seg_dist_prob_filepath, mode='overwrite', sep=',', header=False)
    habit_genre_unify_seg.write.csv(path = habit_genre_unify_filepath, mode='overwrite', sep=',', header=False)

    # # Read back our write files
    # genre_seg_dist_prob_read = gather_data(spark, SEG_DIST_PROB_SCHEMA, genre_seg_dist_prob_filepath, delimiter=',')
    # habit_seg_dist_prob_read = gather_data(spark, SEG_DIST_PROB_SCHEMA, habit_seg_dist_prob_filepath, delimiter=',')
    # habit_genre_unify_seg_read = gather_data(spark, HABIT_GENRE_UNIFY_SEG_SCHEMA, habit_genre_unify_filepath, delimiter=',')


    #############################################
    ### RUNNING REST OF SEGMENTATION PIPELINE ###
    #############################################

    #print('\nRunning Probability Quantiles Module')
    #run_segment_prob_quartile(marketplace_id, 
    #                         marketplace_name.lower(), 
    #                         start_date = start_date_str, 
    #                         end_date = end_date_str, 
    #                         spark = spark, 
    #                         sqlContext = sqlContext)
    #print('\nRunning Transition Probability Module')
    #run_segment_transition_prob(marketplace_name.lower(), 
    #                            start_date1 = start_date_str_2, 
    #                            end_date1 = end_date_str_2, 
    #                            start_date2 = start_date_str, 
    #                            end_date2 = end_date_str,
    #                            spark = spark,
    #                            sqlContext = sqlContext)
    #print('\nRunning Top Title Module')
    #run_top_title_by_segment(marketplace_id, 
    #                         marketplace_name.lower(), 
    #                         start_date = start_date_str, 
    #                         end_date = end_date_str,
    #                         spark = spark,
    #                         sqlContext = sqlContext)
    #print('\nRunning App Stats Module')
    #run_app_stats(marketplace_id, 
    #              marketplace_name.lower(), 
    #              start_date = start_date_str, 
    #              end_date = end_date_str,
    #              spark = spark,
    #              sqlContext = sqlContext)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Beginning Customer Segmentation...")
    parser.add_argument('--marketplace_name', '-m',    help='marketplace name', type=str, required=True)
    parser.add_argument('--marketplace_id', '-i',    help='marketplace id', type=str, required=True)
    parser.add_argument('--end_date',   '-e', help='end date',    type=str, required=True)
    parser.add_argument('--monthly_refresh_rate',   '-mrr', help='monthly refresh rate',    type=int, required=False, default=3)
    parser.add_argument('--yearly_cadence',   '-yc', help='yearly cadence',    type=int, required=False, default=3)
    args = parser.parse_args()

    end_date_str = args.end_date
    monthly_refresh_rate = args.monthly_refresh_rate
    yearly_cadence = args.yearly_cadence
    marketplace_id = args.marketplace_id
    marketplace_name = args.marketplace_name

    marketplace_ids = []
    marketplace_names = []

    # for marketplace_id, marketplace_name, base_label in zip(marketplace_ids, marketplace_names, base_labels):
      # print("Running Quarterly Refresh for %s" %marketplace_name)
    run_segmentation(end_date_str,
                     monthly_refresh_rate,
                     yearly_cadence,
                     marketplace_id,
                     marketplace_name,
                     spark=None, 
                     sqlContext=None)



