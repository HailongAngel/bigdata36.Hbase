package cn.hailong.admin;

import java.io.IOException
;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator.RowComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestAdmin {
	
	
	/**
	 * ���嶼��һ��˼·��
	 * ������Ļ�connect.getAdmin
	 * �������ݵĻ���connect.getTable
	 * ֮�󷵻صĶ��󣬵�����ɾ�Ĳ�ȷ�����������Ҫʲô���ݾʹ�������������ݣ�
	 * 
	 */
	
	/**
	 * ������
	 * @throws Exception
	 */
	@Test
  public void testCreateTable() throws Exception {
		// hadoop�����conf������� hdfs-site.xml core-site.xml,mapred-site.xml ....
				// hadoop ����������ļ� ,������� hbase-site.xml
		
	  Configuration conf = HBaseConfiguration.create(); //���Զ�����hbase-site.xml
	  conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
	  //��ȡ����
	  Connection connect = ConnectionFactory.createConnection(conf);
	  //�������л�ȡ��������
	  Admin admin = connect.getAdmin();
	  
	  
	  //������
	  HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("t_user"));
	  //��������
	  HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("base_info");
	  HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("family");
	  //
	  hTableDescriptor.addFamily(hColumnDescriptor);
	  hTableDescriptor.addFamily(hColumnDescriptor2);
	  
	  //����table
	  admin.createTable(hTableDescriptor);
	  //�ر���Դ
	  admin.close();
      connect.close();
	  
  }
	/**
	 * ɾ����
	 * @throws Exception
	 */
	@Test
	public void testDeleteTable() throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//�����ӻ�ȡ��������
		Admin admin = connect.getAdmin();
		//����Ϊ������
		admin.disableTable(TableName.valueOf("t_user"));
		//ɾ����
		admin.deleteTable(TableName.valueOf("t_user"));
		admin.close();
		connect.close();
		
		
	}
	/**
	 * �޸ı���
	 * @throws IOException 
	 */
	@Test
	public void testAlterTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//�����ӻ�ȡ��������
		Admin admin = connect.getAdmin();
		 //������
		HTableDescriptor user = new HTableDescriptor(TableName.valueOf("t_user"));
		//��������
		 HColumnDescriptor f3 = new HColumnDescriptor("f3");
		  user.addFamily(f3);
		  //�޸ı���
		admin.modifyTable(TableName.valueOf("t_user"),user );
		admin.close();
        connect.close();
	}
	/**
	 * ��������
	 * @throws Exception 
	 */
	/*@Test
	public void testadd() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		Table table = connect.getTable(TableName.valueOf("t_user"));
		//key
		Put put = new Put("001".getBytes());
		Put put1 = put.addColumn("f3".getBytes(), "name".getBytes(), "lihailong".getBytes());
		Put put2 = put.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("18"));
		
		Put put00 = new Put("002".getBytes());
		Put put3 = put00.addColumn("f3".getBytes(), "name".getBytes(), "lihailong1".getBytes());
		Put put4 = put00.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("10"));
		Put put01 = new Put("003".getBytes());
		Put put5 = put01.addColumn("f3".getBytes(), "name".getBytes(), "lihailong2".getBytes());
		Put put6 = put01.addColumn("f3".getBytes(), Bytes.toBytes("age"),Bytes.toBytes("180"));
		
		List<Put> list = new ArrayList();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		list.add(put4);
		list.add(put5);
		list.add(put6);
		
		table.put(list);
		table.close();
		connect.close();
	}
	
	*//**
	 * ɾ������
	 * @throws IOException 
	 *//*
	@Test
	public void testDelete() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		Table table = connect.getTable(TableName.valueOf("test_user"));
		Delete delete = new Delete(Bytes.toBytes("001"));
		delete.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"));
		table.delete(delete);
		
		table.close();
		connect.close();
		
	}
	
	
	*//**
	 * ���ݲ�ѯ
	 * @throws IOException 
	 *//*
	@Test
	public void getData() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Get get = new Get(Bytes.toBytes("001"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
		System.out.println(new String(value));
		byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
		System.out.println(new String(value1));
		table.close();
		connect.close();
		
	}
	*//**
	 * 
	 * ���в�ѯ
	 * @throws IOException 
	 * 
	 *//*
	@Test
	public void getScan() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//����ͷ������β//�����õĻ���ȫ��ɨ��
		scan.setStartRow(Bytes.toBytes("001"));
		scan.setStopRow(Bytes.toBytes("002"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		table.close();
        connect.close();
	}
	
	
	*//**
	 * ��ֵ������
	 * @throws IOException 
	 * 
	 *//*
	@Test
	public void testSingleColumnValueFilter() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//��ȡ�������ݵĶ���
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//��ֵ������
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
		table.close();
        connect.close();
	}
	
	*//**
	 * 
	 * ����ǰ׺������
	 * @throws IOException 
	 *//*
	@Test
	public void testColumnPrefixFilter() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//��ȡ�������ݵĶ���
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//����ǰ׺
		ColumnPrefixFilter prefix = new ColumnPrefixFilter(Bytes.toBytes("name"));
		//��scan������ֵ������
		scan.setFilter(prefix);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		table.close();
        connect.close();
	}
	*//**
	 * �����ֵǰ׺������
	 * @throws IOException 
	 *//*
	@Test
	public void testColumnPrefixFilterMore() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//��ȡ�������ݵĶ���
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//�����ֵǰ׺
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
		table.close();
        connect.close();
	}
	*//**
	 * rowKey������
	 * @throws IOException 
	 *//*
	@Test
	public void testRowFilter() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//��ȡ�������ݵĶ���
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		//rowKey������
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
		table.close();
        connect.close();
	}
	*//**
	 * �������б�
	 * @throws IOException 
	 *//*
	@Test
	public void testFilterList() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		//��ȡ����
		Connection connect = ConnectionFactory.createConnection(conf);
		//��ȡ�������ݵĶ���
		Table table = connect.getTable(TableName.valueOf("t_user"));
		Scan scan = new Scan();
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^001")));
		filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("name")));
		
		//��scan������ֵ������
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("name"));
			System.out.println(new String(value));
			byte[] value1 = result.getValue(Bytes.toBytes("f3"), Bytes.toBytes("age"));
			System.out.println(new String(value1));
			
		}
		table.close();
        connect.close();
	}
		*/
	}
