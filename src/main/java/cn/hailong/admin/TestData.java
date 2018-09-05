package cn.hailong.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestData {
	/*
	 *对数据的增删改查以及过滤器的使用
	 */
	
	/**
	 * 	//获取连接
	 */
	Configuration conf = null;
	Connection connect = null;
	Table table = null;
	@Before
public void init() throws IOException {
	conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
	connect = ConnectionFactory.createConnection(conf);
}
	
	/**
	 * 增加数据
	 * @throws Exception 
	 */
	@Test
	public void testadd() throws Exception {
		 table = connect.getTable(TableName.valueOf("t_user"));
		//设置rowkey
		Put put = new Put("001".getBytes());
		//增加列族、列、值
		Put put1 = put.addColumn("f3".getBytes(), "name".getBytes(), "lihailong".getBytes());
		Put put2 = put.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("18"));
		
		Put put00 = new Put("002".getBytes());
		Put put3 = put00.addColumn("f3".getBytes(), "name".getBytes(), "lihailong1".getBytes());
		Put put4 = put00.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("10"));
		
		Put put01 = new Put("003".getBytes());
		Put put5 = put01.addColumn("f3".getBytes(), "name".getBytes(), "lihailong2".getBytes());
		Put put6 = put01.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("180"));
		//放到list中
		List<Put> list = new ArrayList();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		list.add(put4);
		list.add(put5);
		list.add(put6);
		//增加数据
		table.put(list);
		
	}
	
	/**
	 * 删除数据
	 * @throws IOException 
	 */
	@Test
	public void testDelete() throws IOException {
		 table = connect.getTable(TableName.valueOf("test_user"));
		//删除哪一个key
		Delete delete = new Delete(Bytes.toBytes("001"));
		//删除哪个列、哪个属性
		delete.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"));
		//删除
		table.delete(delete);
	}
	
	
	/**
	 * 数据查询
	 * @throws IOException 
	 */
	@Test
	public void getData() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		//查询rowkey为001的数据
		Get get = new Get(Bytes.toBytes("001"));
		//调用查询的方法
		Result result = table.get(get);
		//获取查询到的value
		byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
		System.out.println(new String(value));
		byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
		System.out.println(new String(value1));
	
		
	}
	/**
	 * 
	 * 多行查询
	 * @throws IOException 
	 * 
	 */
	@Test
	public void getScan() throws IOException {
	   
		 table = connect.getTable(TableName.valueOf("t_user"));
		//创建新的扫描
		Scan scan = new Scan();
		//包含头不包含尾//不设置的话，全表扫描
		scan.setStartRow(Bytes.toBytes("001"));
		scan.setStopRow(Bytes.toBytes("002"));
		//获取多行查询的结果
		ResultScanner scanner = table.getScanner(scan);
		//遍历所得到的值
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		
	}
	
	
	/**
	 * 列值过滤器
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testSingleColumnValueFilter() throws IOException {
		//获取控制数据的对象
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//列值过滤器，后面的值分别是列族、列、如何比较、属性值
		SingleColumnValueExcludeFilter clo = new SingleColumnValueExcludeFilter(Bytes.toBytes("f3"), Bytes.toBytes("age"), CompareOp.EQUAL, Bytes.toBytes("18"));
		//给scan设置列值过滤器
		scan.setFilter(clo);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
	
	}
	
	/**
	 * 
	 * 列名前缀过滤器
	 * @throws IOException 
	 */
	@Test
	public void testColumnPrefixFilter() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//列名前缀
		ColumnPrefixFilter prefix = new ColumnPrefixFilter(Bytes.toBytes("name"));
		//给scan设置列值过滤器
		scan.setFilter(prefix);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			
		}
	
	}
	/**
	 * 多个列值前缀过滤器
	 * @throws IOException 
	 */
	@Test
	public void testColumnPrefixFilterMore() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//多个列值前缀，增加多个列值前缀
		byte[][] prefixs = new byte[][] {Bytes.toBytes("name"),Bytes.toBytes("age")};
		MultipleColumnPrefixFilter prefix = new MultipleColumnPrefixFilter(prefixs);
		//给scan设置列值过滤器
		scan.setFilter(prefix);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
	}
	/**
	 * rowKey过滤器
	 * @throws IOException 
	 */
	@Test
	public void testRowFilter() throws IOException {
	
		//获取控制数据的对象
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//rowKey过滤器，找到与后面正则表达式相等的rowkey
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^001"));
		//给scan设置列值过滤器
		scan.setFilter(rowFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		
	}
	/**
	 * 过滤器列表,多个过滤器混合起来使用
	 * @throws IOException 
	 */
	@Test
	public void testFilterList() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//里面的方法设置是过滤器全部使用
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^001")));
		filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("name")));
		
		//给scan设置列值过滤器
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			/*byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));*/
			
		}
		
	}
	
	@After
		public void end() throws IOException {
			table.close();
	        connect.close();
		}
	}

