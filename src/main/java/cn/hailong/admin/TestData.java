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
	 *�����ݵ���ɾ�Ĳ��Լ���������ʹ��
	 */
	
	/**
	 * 	//��ȡ����
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
	 * ��������
	 * @throws Exception 
	 */
	@Test
	public void testadd() throws Exception {
		 table = connect.getTable(TableName.valueOf("t_user"));
		//����rowkey
		Put put = new Put("001".getBytes());
		//�������塢�С�ֵ
		Put put1 = put.addColumn("f3".getBytes(), "name".getBytes(), "lihailong".getBytes());
		Put put2 = put.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("18"));
		
		Put put00 = new Put("002".getBytes());
		Put put3 = put00.addColumn("f3".getBytes(), "name".getBytes(), "lihailong1".getBytes());
		Put put4 = put00.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("10"));
		
		Put put01 = new Put("003".getBytes());
		Put put5 = put01.addColumn("f3".getBytes(), "name".getBytes(), "lihailong2".getBytes());
		Put put6 = put01.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("180"));
		//�ŵ�list��
		List<Put> list = new ArrayList();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		list.add(put4);
		list.add(put5);
		list.add(put6);
		//��������
		table.put(list);
		
	}
	
	/**
	 * ɾ������
	 * @throws IOException 
	 */
	@Test
	public void testDelete() throws IOException {
		 table = connect.getTable(TableName.valueOf("test_user"));
		//ɾ����һ��key
		Delete delete = new Delete(Bytes.toBytes("001"));
		//ɾ���ĸ��С��ĸ�����
		delete.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"));
		//ɾ��
		table.delete(delete);
	}
	
	
	/**
	 * ���ݲ�ѯ
	 * @throws IOException 
	 */
	@Test
	public void getData() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		//��ѯrowkeyΪ001������
		Get get = new Get(Bytes.toBytes("001"));
		//���ò�ѯ�ķ���
		Result result = table.get(get);
		//��ȡ��ѯ����value
		byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
		System.out.println(new String(value));
		byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
		System.out.println(new String(value1));
	
		
	}
	/**
	 * 
	 * ���в�ѯ
	 * @throws IOException 
	 * 
	 */
	@Test
	public void getScan() throws IOException {
	   
		 table = connect.getTable(TableName.valueOf("t_user"));
		//�����µ�ɨ��
		Scan scan = new Scan();
		//����ͷ������β//�����õĻ���ȫ��ɨ��
		scan.setStartRow(Bytes.toBytes("001"));
		scan.setStopRow(Bytes.toBytes("002"));
		//��ȡ���в�ѯ�Ľ��
		ResultScanner scanner = table.getScanner(scan);
		//�������õ���ֵ
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		
	}
	
	
	/**
	 * ��ֵ������
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testSingleColumnValueFilter() throws IOException {
		//��ȡ�������ݵĶ���
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//��ֵ�������������ֵ�ֱ������塢�С���αȽϡ�����ֵ
		SingleColumnValueExcludeFilter clo = new SingleColumnValueExcludeFilter(Bytes.toBytes("f3"), Bytes.toBytes("age"), CompareOp.EQUAL, Bytes.toBytes("18"));
		//��scan������ֵ������
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
	 * ����ǰ׺������
	 * @throws IOException 
	 */
	@Test
	public void testColumnPrefixFilter() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//����ǰ׺
		ColumnPrefixFilter prefix = new ColumnPrefixFilter(Bytes.toBytes("name"));
		//��scan������ֵ������
		scan.setFilter(prefix);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			
		}
	
	}
	/**
	 * �����ֵǰ׺������
	 * @throws IOException 
	 */
	@Test
	public void testColumnPrefixFilterMore() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//�����ֵǰ׺�����Ӷ����ֵǰ׺
		byte[][] prefixs = new byte[][] {Bytes.toBytes("name"),Bytes.toBytes("age")};
		MultipleColumnPrefixFilter prefix = new MultipleColumnPrefixFilter(prefixs);
		//��scan������ֵ������
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
	 * rowKey������
	 * @throws IOException 
	 */
	@Test
	public void testRowFilter() throws IOException {
	
		//��ȡ�������ݵĶ���
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//rowKey���������ҵ������������ʽ��ȵ�rowkey
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^001"));
		//��scan������ֵ������
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
	 * �������б�,����������������ʹ��
	 * @throws IOException 
	 */
	@Test
	public void testFilterList() throws IOException {
		
		 table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//����ķ��������ǹ�����ȫ��ʹ��
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^001")));
		filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("name")));
		
		//��scan������ֵ������
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

