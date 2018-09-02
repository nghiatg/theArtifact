package mp;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class MainClass {
	static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("From_Hdfs_To_DB").set("spark.sql.parquet.binaryAsString",
			"true");
	static JavaSparkContext jsc = new JavaSparkContext(conf);
	static SQLContext sqlc = new SQLContext(jsc);
	static String cvModelPath = "";
	static Connection conn = null;
	static String[] vocabulary = null;
	static {
		try {
			conn = connectoToDB();
			vocabulary = getVocab(loadCvModel());
		} catch (Exception e) {

		}
	};
	
	
	// args
	//		0 : tfidf_parquet_path
	//		1 : cvModel_path
	public static void main(String[] args) throws Exception {
		jsc.setLogLevel("ERROR");
		setCvModelPath(args[1]);
		idk(args[0]);
	}
	
	public static void setCvModelPath(String path) {
		cvModelPath = path;
	}

	public static CountVectorizerModel loadCvModel() throws Exception {
		return CountVectorizerModel.load(cvModelPath);
	}

	public static DataFrame readParquet(String path) throws Exception {
		return sqlc.read().parquet(path);
	}

	public static DataFrame readParquet(String[] paths) throws Exception {
		return sqlc.read().parquet(paths);
	}

	public static String[] getVocab(CountVectorizerModel cvModel) throws Exception {
		return cvModel.vocabulary();
	}

	public static class MyFunction implements VoidFunction<Row>, Serializable {

		public void call(Row r) throws Exception {
			String url = r.getString(2);
			Integer cate = Integer.parseInt(r.getString(3));
			Double percent = Double.parseDouble(r.getString(4));
			if (r.getAs(7) instanceof SparseVector) {
				double[] features = ((SparseVector) r.getAs(7)).toArray();
				ArrayList<Integer> idMax = findMax(features, 10);
				StringBuilder kws = new StringBuilder();
				for (int i : idMax) {
					kws.append(vocabulary[i]).append(",");
				}
				conn.createStatement()
					.executeQuery("insert into news_title_kw values('" + url + "'," + cate + ","
									+ percent + ",'" + kws.substring(0, kws.length() - 1) + "')");
			}
		}

	}

	public static void idk(String parquetPath) throws Exception {
		DataFrame df = readParquet(parquetPath);
		df.registerTempTable("df");
		DataFrame filtered = sqlc.sql("select url,cate,features from df where cate <> '600' and cate <> '404'");
		filtered.cache();
		MyFunction mf = new MyFunction();
		filtered.toJavaRDD().foreach(mf);
	}

	public static ArrayList<Integer> findMax(double[] arr, int number) {
		ArrayList<Integer> idMax = new ArrayList<Integer>();
		ArrayList<Double> list = new ArrayList<Double>();
		for (double d : arr) {
			list.add(d);
		}
		Collections.sort(list, Collections.reverseOrder());
		for (int i = 0; i < number;) {
			for (int id = 0; id < arr.length; ++id) {
				if (arr[id] == list.get(i)) {
					idMax.add(id);
					++i;
					if (i >= number) {
						break;
					}
				}
			}
		}
		return idMax;

	}

	public static Connection connectoToDB() throws Exception {
		String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8";
		String user = "root";
		String password = "";
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

}
