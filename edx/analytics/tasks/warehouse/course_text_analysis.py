"""Categorize courses."""

import luigi

from edx.analytics.tasks.common.spark import BasicSparkJobTask
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url
# Try to first write a task with little additional overhead, so leave this out for now.
# from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


class ClusterCoursesByTextTask(BasicSparkJobTask):
    """
    Clusters courses according to the text provided from the course content.

    Input format is a TSV with three columns:   course_id, num_blocks, text.
    """
    
    input_path = luigi.Parameter()
    num_clusters = luigi.IntParameter(default=4)
    output_path = luigi.Parameter(default=None)
    num_top_words = luigi.IntParameter(default=25)

    def __init__(self, *args, **kwargs):
        super(ClusterCoursesByTextTask, self).__init__(*args, **kwargs)
        if self.output_path == None:
            self.output_path = "{}.{}_clusters".format(self.input_path, self.num_clusters)

    def requires(self):
        return ExternalURL(url=self.input_path)

    def output(self):
        # TODO: this should actually check for _SUCCESS in output_path.
        return get_target_from_url(self.output_path)
            
    def spark_job(self, *args):
        # Include pyspark packages here, since they exist within the Spark runtime but not in the
        # virtualenv that Luigi uses.
        from pyspark.ml.clustering import BisectingKMeans
        from pyspark.ml.feature import CountVectorizer, IDF, RegexTokenizer, StopWordsRemover
        from pyspark.sql import Row
        from pyspark.sql.types import StructType, StringType, IntegerType

        schema = StructType() \
                 .add("course_id", StringType(), True) \
                 .add("count", IntegerType(), True) \
                 .add("text", StringType(), True)

        # This failed in the local shell.
        input_df = self._spark.read.csv(self.input_path, header=False, schema=schema, sep="\t", mode='FAILFAST')

        input_df.printSchema()
        input_df.select('course_id', 'count').show(10)

        def print_fcn(str):
            print str
            # self.log.warn(str)

        reTokenizer = RegexTokenizer(inputCol='text', outputCol='tokens', minTokenLength=3, gaps=False, pattern='\\b[a-zA-Z][a-zA-Z]+\\b')
        tokenized_df = reTokenizer.transform(input_df).select('course_id', 'count', 'tokens')

        remover = StopWordsRemover(inputCol='tokens', outputCol='stopped')
        stopped_df = remover.transform(tokenized_df).select('course_id', 'count', 'stopped')

        # I'll skip the ngrams for now, though there's a separate class for that.
        # Maybe add bigrams later, if it seems computationally tractable.
        
        cv = CountVectorizer(inputCol="stopped", outputCol="vectors")
        cv_model = cv.fit(stopped_df)
        vocabulary = cv_model.vocabulary
        print_fcn("Vocabulary size:  %d" % len(vocabulary))
        cv_df = cv_model.transform(stopped_df).select('course_id', 'count', 'vectors')

        idf2 = IDF(inputCol="vectors", outputCol="idf")
        idfModel2 = idf2.fit(cv_df)
        idf_df = idfModel2.transform(cv_df).select('course_id', 'count', 'idf')

        # TODO: fix "The input RDD 30 is not directly cached, which may hurt performance if its parent RDDs are also not cached."
        # This cache call doesn't do the trick.
        idf_df.cache()

        kmeans = BisectingKMeans(featuresCol='idf', k=self.num_clusters, predictionCol='cluster')
        km_model = kmeans.fit(idf_df)

        # Output report about what words and what courses are in which cluster.
        km_centers = km_model.clusterCenters()
        print_fcn("Found %d clusters" % len(km_centers))
        print_fcn("Used k value of %d" % km_model.summary.k)
        print_fcn("Cluster sizes found {}".format(km_model.summary.clusterSizes))
        print_fcn("Centers:")

        for index, center in enumerate(km_centers):
            center_ordered = center.argsort()
            center_revordered = center_ordered[::-1]
            top_indices = center_revordered[:self.num_top_words]
            top_vocab = [cv_model.vocabulary[idx] for idx in top_indices]
            vocab_string = ', '.join(top_vocab)
            print_fcn("Centroid %d: %s " % (index, vocab_string))

        output_df = km_model.transform(idf_df)
        result = output_df.select('cluster', 'course_id', 'count').orderBy(['cluster', 'course_id'], ascending=[1,1])

        # persist the output.
        # output_path = self.output_dir().path
        output_path = self.output_path

        # Output is to a directory, not to a file.  Filename will be part-00000-{uuid}.csv.
        print_fcn("Attempting to write CSV file output to directory {}".format(output_path))
        # why use mode='append'?
        # Use repartition, to avoid having upstream processing also use only one partition.
        # But we lose the sorting.
        # result.coalesce(1).write.csv(output_path, sep='\t')
        result.repartition(1).write.csv(output_path, sep='\t')

        print_fcn("")
        print_fcn("Output cluster memberships...")
        for index, center in enumerate(km_centers):
            center_ordered = center.argsort()
            center_revordered = center_ordered[::-1]
            top_indices = center_revordered[:self.num_top_words]
            top_vocab = [cv_model.vocabulary[idx] for idx in top_indices]
            vocab_string = ', '.join(top_vocab)
            print_fcn("Centroid %d: %s " % (index, vocab_string))
            members_df = result.filter(result['cluster'] == index).select('course_id')
            members_df.show(truncate=False, n=members_df.count())
