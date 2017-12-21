# DataPorter

## 背景

我们之前是使用 talend 进行数据导入导出

talend 是一个通过拖拽组件生成代码的IDE

虽然使用简单，

但是重复性配置以及代码管理非常麻烦，

无法通过git等把控我们的改动内容

并且组件我们无法改动，当与hadoop环境等不一致时，解决问题非常麻烦

所以我们需要一个代码可控，使用简单的组件

由此我们写出了 DataPorter 这个框架

## 目前所支持的功能

为了支持功能的可插拔，我们将 package 作为功能的划分， 一类功能只会存在于一个package 中

所以用户可以根据需要自行选择 package


* CSV

    提供本地 CSV 文件的读取为DataSet以及从DataSet写入本地 CSV 文件的功能

    library ( sbt 风格 ): ```"com.newegg.eims" %% "dataporter-csv" % dataporterVersion``` 

* Hive

    提供从hive thrift server 1 以及 hive thrift server 2 读取数据为DataSet的功能

    library ( sbt 风格 ): ```"com.newegg.eims" %% "dataporter-hive" % dataporterVersion```

* Parquet

    提供 parquet 文件的读取为DataSet以及从DataSet写入 parquet 文件的功能

    library ( sbt 风格 ): ```"com.newegg.eims" %% "dataporter-parquet" % cdhVersion```

* Sql

    提供从db通过 jdbc 读取数据为DataSet以及从DataSet通过 jdbc 写入db的功能

    由于基于jdbc，所以具体使用sqlserver，mysql 还是其他需要用户自行引入jdbc驱动

    library ( sbt 风格 ): ```"com.newegg.eims" %% "dataporter-base" % dataporterVersion``` 

* Base

    提供各类DataSet的扩展函数操作

    library ( sbt 风格 ): ```"com.newegg.eims" %% "dataporter-base" % dataporterVersion``` 

**PS：** 

dataporterVersion 最新为 0.35

cdhVersion 最新为 0.35-1.6.0-cdh5.8.3

所有扩展函数都在package对应的 Converts 类中，这样方便大家使用

## 简要设计说明

### 核心抽象

DataPorter 核心抽象 只有三个：

* DataSet 抽象数据集的接口

    DataSet 为行列结构简单统一，很方便框架处理
    
    其次为实现懒操作，不持有真实数据，只有在实际迭代遍历开始时才会读取数据，
    
    并且继承于Iterable， 以便可以使用scala的各种集合操作函数

* IDataColumn 列接口

    承载数据类型定义以及数据处理实现

* IDataRow 行接口

    提供行的抽象以及自定义类的对应抽象

将不同的数据读取处理封装到对应的Dataset（比如csv文件的读取），这样我们就能在代码中操作集合一样操作csv文件读出的数据了

### 特殊的 StructSet

面向对象的开发模式中，我们喜欢将数据抽象为class，

class也可以说是数据的列定义集合，但是有了名字作为数据归类划分，让我们理解代码更为方便

所以为了dataset也有这样的便利，StructSet便诞生了

StructSet 只做两件事： 把class转换为行列结构 以及 把行列结构转发为class

当然由于涉及反射，略微会有性能损失，但大部分情况都可以接受


## 框架 API 文档

规定的类型列表：

|Data type  |	Value type |
| ---        | --- |
|CByte	    |Byte	|
|CShort	    |Short	|
|CInt	    |Int	|
|CLong	    |Long	|
|CFloat	    |Float	|
|CDouble	|Double	|
|CBigdecimal|java.math.BigDecimal	|
|CString	|String	|
|CBytes 	|Array[Byte]	|
|CBoolean	|Boolean	|
|CTimestamp	|java.sql.Timestamp	|
|CDate	    |java.sql.Date	|
|CArray	    |scala.collection.Seq	|
|CMap	    |scala.collection.Map	|
|CStruct	| IDataRow |

## Code Example

``` scala

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base.DataColumn
import com.newegg.eims.DataPorter.Csv.Converts._
import com.newegg.eims.DataPorter.Parquet.Converts._
import com.newegg.eims.DataPorter.Sql.SqlDataSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
 
case class TestD1(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String) {
    val c : Boolean
    @(Column@field)(LIST(CInt)) var d: Array[Int] = _
}
object test {
 
 
  def main(args: Array[String]): Unit = {
      //获取sql中数据
    val s = new SqlDataSet("jdbc:sqlserver://xx\\xx;DatabaseName=xx;user=xx;password=xx",
      "select top 3 programnumber,null as a from dbo.eimsprogram with(nolock)")
 
    val temp = s
       // 可转化为带结构描述的数据集
      .transform(
        new DataColumn(0, "b", CInt, r => Some(r.getValue[Int](1).getOrElse(1) * 5)),
        new DataColumn(1, "programnumber", CInt, r => r.getVal("programnumber"))
      )
      // 也可转化为不带结构描述的数据集
      // scala 的 集合函数都可以使用，比如filter，foreach 等等
      .map(r => (r.getValue(0).getOrElse(0), r.getValue(1).getOrElse(0)))
      // 也可以再次转化为带结构描述的数据集
      .transform(
        new DataColumn(0, "a", CInt, r => Some(r._1)),
        new DataColumn(1, "b", CInt, r => Some(r._2))
      )
 
    // 0.8 版本之后支持转化为实体, 要求必须打上字段级别的注解，没有注解的字段会忽略处理,例如： @(Column@field)(CInt) ，TestD1 中的c 字段会忽略
    // 构造器中字段要求必须有对应值，否则无法实例化，
    // case class 在 scala 2.11 之前版本有22个最多字段的限制
    // Column DataSet 转 StructDataSet
    temp.as[TestD1]().foreach(r => println(r.b))
    // 实体集合转StructDataSet
    val data = (Array(1, 2, 3, 4, 5, 6).map(i => TestD1(i * 5, i.toString())).toIterable.asDataSet()
 
    temp.foreach(r => println(r.getValue[Int]("b").getOrElse(0)))
    // 支持储存为csv 格式
    temp.saveCsv("C:\\Users\\VQ83\\Desktop\\TEMP\\New folder\\MerchantType3.csv", isAppend = true)
    // 支持储存为 apache orc 格式
    temp.saveParquet("C:\\Users\\VQ83\\Desktop\\TEMP\\New folder\\MerchantType3.parquet")
  }
}
```

## 开发

开发语言采用 **scala**  （版本 2.10.6）

开发管理工具采用 **sbt** （版本 0.13.8）

### coverage

``` bash
sbt clean coverage test coverageReport coverageAggregate coverageOff
```

### publish package to maven

``` bash
sbt publish
```

### generate doc

``` bash
sbt unidoc
```
