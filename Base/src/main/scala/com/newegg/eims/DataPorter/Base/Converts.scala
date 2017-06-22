package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.ReflectHelper.universe._

import scala.reflect.ClassTag

/**
  * The Converts method of DataSet
  */
object Converts {

  /**
    * implicit class when Iterable data convert to DataSet[IDataRow]
    *
    * @param iterable
    * @tparam T
    */
  implicit class IterableToDataSet[T](val iterable: Iterable[T]) {

    /**
      * Iterable data convert to DataSet[IDataRow]
      *
      * @param dataColumns The iterable data how to convert to DataSet[IDataRow]
      * @return DataSet[IDataRow]
      */
    def transform(dataColumns: DataColumn[T]*): DataSet[IDataRow] = {
      val cols = dataColumns.map(i => (i.getIndex, i)).toMap
      val schema = new DataSetSchema(cols)
      new TransformDataSet[T](iterable, schema)
    }
  }

  /**
    * implicit class when Iterable data convert to StructSet[T, T]
    *
    * @param iterable
    * @param ev$1
    * @param ev$2
    * @param c
    * @tparam T
    */
  implicit class IterableToStructSet[T: TypeTag : ClassTag](val iterable: Iterable[T])(implicit c: T <:< Any) {
    /**
      * Iterable data convert to StructSet[T, T]
      *
      * @return StructSet[T, T]
      */
    def asDataSet(): StructSet[T, T] = {
      val (schema, struct) = ReflectHelper.createSchema[T]()
      new StructSet[T, T](iterable, schema, r => r, struct.toRow)
    }
  }

  /**
    * implicit class to help get struct info from CStruct[T]
    *
    * @param cStruct
    * @param ev$1
    * @param ev$2
    * @param c
    * @tparam T
    */
  implicit class CStructHelper[T: TypeTag : ClassTag](val cStruct: CStruct[T])(implicit c: T <:< Any) {
    /**
      * get struct info from CStruct[T]
      *
      * @return ([[DataSetSchema]], [[CStructInfo]])
      */
    def getSchema: (DataSetSchema, CStructInfo) = ReflectHelper.createSchema[T]()
  }

  /**
    * implicit class when DataSet[IDataRow] convert to StructSet[T, IDataRow]
    *
    * @param set
    */
  implicit class RowToStructSet(val set: DataSet[IDataRow]) {

    /**
      * DataSet[IDataRow] convert to StructSet[T, IDataRow]
      *
      * @param ev
      * @tparam T the class type which you want to convert
      * @return StructSet[T, IDataRow]
      */
    def as[T: TypeTag : ClassTag]()(implicit ev: Null <:< T): StructSet[T, IDataRow] = {
      val (schema, struct) = ReflectHelper.createSchema[T]()
      new StructSet[T, IDataRow](set, schema, r => {
        val v = struct.creator(r)
        if (v.isDefined) v.get.asInstanceOf[T] else ev(null)
      }, struct.toRow)
    }
  }

  /**
    * implicit class when DataSet[T] want to do mapTo or union
    *
    * @param set
    * @tparam T
    */
  implicit class MapToDataSet[T](val set: DataSet[T]) {

    /**
      * create a new DataSet[IDataRow] base on some already exist DataSet[T], and can do change with the cols
      *
      * @param removeCols        columns which no need
      * @param newOrOverrideCols columns which is new or different of the old
      * @return DataSet[IDataRow]
      */
    def mapTo(removeCols: Set[String] = Set.empty[String], newOrOverrideCols: Array[DataColumn[T]]): DataSet[IDataRow] = {
      new MapDataSet[T](set, removeCols.map(i => i.toLowerCase), newOrOverrideCols)
    }

    /**
      * replace None with defulat value by columns type
      * @return NoneToDefulatValueDataSet[T]
      */
    def noneToDefulatValue(): NoneToDefulatValueDataSet[T] = {
      new NoneToDefulatValueDataSet[T](set)
    }

    /**
      * union all of some DataSets, make it like one DataSet
      *
      * @param dataSets The DataSets which has same schema
      * @return UnionDataSet[T]
      */
    def union(dataSets: DataSet[T]*): UnionDataSet[T] = {
      val ds = dataSets.toBuffer
      ds.insert(0, set)
      new UnionDataSet(ds: _*)
    }
  }

}
