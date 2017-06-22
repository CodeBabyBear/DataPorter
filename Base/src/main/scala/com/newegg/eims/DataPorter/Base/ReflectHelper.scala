package com.newegg.eims.DataPorter.Base

import java.math.RoundingMode

import scala.reflect.ClassTag

/**
  * Reflect Helper to handle struct and Column type
  */
object ReflectHelper {

  /**
    * universe from runtime, use this just avoid scala Reflect not threading safe in 2.10 problem
    */
  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe

  import universe._

  private val columnType = typeOf[Column]

  /**
    * get mirror with synchronized lock
    *
    * @return universe.Mirror
    */
  def mirror: universe.Mirror = ScalaReflectionLock.synchronized {
    scala.reflect.runtime.currentMirror
  }

  /**
    * get Constructor from type t with synchronized lock
    *
    * @param t Type
    * @return (MethodMirror, List[String])
    */
  def getConstructor(t: Type): (MethodMirror, List[String]) = ScalaReflectionLock.synchronized {
    val m = mirror
    val cm = m.reflectClass(t.typeSymbol.asClass)
    val ctor = t.declaration(universe.nme.CONSTRUCTOR).asMethod
    (cm.reflectConstructor(ctor), ctor.paramss.head.map(i => getRealName(i.name)))
  }

  /**
    * get fields from type t with synchronized lock
    *
    * @param t Type
    * @return Iterable[universe.TermSymbol]
    */
  def getFields(t: Type): Iterable[universe.TermSymbol] = ScalaReflectionLock.synchronized {
    t.members.map {
      case m: universe.TermSymbol if m.isVar || m.isVal => m
      case _ => null
    } filter (i => i != null)
  }

  /**
    * keep column type convert info
    *
    * @param colType column type
    * @param convert data convert to the column type
    * @param toRow   column data convert to row
    */
  case class ColConvertor(colType: ColumnType, convert: Any => Any = r => r, toRow: Any => Any = r => r)

  /**
    * convert field type tree to ColConvertor with synchronized lock
    *
    * @param tree field type tree
    * @return ColConvertor
    */
  def treeToColType(tree: Tree): ColConvertor = ScalaReflectionLock.synchronized {
    val str = treeToStr(tree)
    anyToColType(str)
  }

  /**
    * convert info which get from treeToStr to ColConvertor
    *
    * @param str info which get from [[treeToStr]]
    * @return ColConvertor
    */
  def anyToColType(str: Any): ColConvertor = str match {
    case "CBool" => ColConvertor(CBool)
    case "CInt" => ColConvertor(CInt)
    case "CLong" => ColConvertor(CLong)
    case "CShort" => ColConvertor(CShort)
    case "CByte" => ColConvertor(CByte)
    case "CBytes" => ColConvertor(CBytes)
    case "CFloat" => ColConvertor(CFloat)
    case "CDouble" => ColConvertor(CDouble)
    case "CString" => ColConvertor(CString)
    case "CTimestamp" => ColConvertor(CTimestamp)
    case "CDate" => ColConvertor(CDate)
    case (("CBigdecimal", _), List(precision, scale)) =>
      val p = precision match {
        case pp: String => pp.toInt
        case _ => 18
      }
      val s = scale match {
        case pp: String => pp.toInt
        case _ => 2
      }
      ColConvertor(CBigdecimal(p, s), {
        case x: java.math.BigDecimal => x.setScale(s, RoundingMode.HALF_UP)
        case r => r
      })
    case (("CMap", _), List(x, y)) =>
      val k = anyToColType(x)
      val v = anyToColType(y)
      ColConvertor(CMap(k.colType, v.colType), {
        case z: Iterable[(_, _)] => z.map(i => k.convert(i._1) -> v.convert(i._2)).toMap
        case _ => null
      }, {
        case z: Iterable[(_, _)] => z.map(i => k.toRow(i._1) -> v.toRow(i._2))
        case _ => null
      })
    case (("CList", _), List(x)) =>
      val t = anyToColType(x)
      ColConvertor(CList(t.colType), {
        case y: Iterable[_] => y.map(t.convert).toSeq
        case _ => null
      }, {
        case y: Iterable[_] => y.map(t.toRow).toSeq
        case _ => null
      })
    case ((("CStruct", _), List(t: Type)), _) =>
      val struct = createCStructInfo(t)
      ColConvertor(struct, {
        case r: IDataRow => struct.creator(r).orNull
        case _ => null
      }, struct.toRow)
    case _ =>
      ColConvertor(CNoSupport)
  }

  /**
    * convert TermSymbol to ColConvertor with synchronized lock
    *
    * @param term TermSymbol
    * @return ColConvertor
    */
  def getColType(term: TermSymbol): ColConvertor = ScalaReflectionLock.synchronized {
    val col = term.annotations.find(i => i.tpe =:= columnType)
    if (col.isDefined)
      ReflectHelper.treeToColType(col.get.scalaArgs.head)
    else ColConvertor(CNoSupport)
  }

  /**
    * create CStructInfo for t with synchronized lock
    *
    * @param t type
    * @return [[CStructInfo]]
    */
  def createCStructInfo(t: Type): CStructInfo = ScalaReflectionLock.synchronized {
    val (ctorm, arr) = getConstructor(t)
    val (vals, vars) = getValAndVar(t)
    val allfs = Array(vals, vars).map(i => i.values.map(i => (i, getColType(i), getRealName(i.name)))
      .filterNot(i => i._2.colType == CNoSupport).toArray.sortBy(i => i._3))
    val getfs = allfs.head
    val setfs = allfs(1)
    val getMap = getfs.map(i => i._3.toLowerCase -> i).toMap
    val setMap = setfs.map(i => i._3.toLowerCase -> i).toMap
    val createStruct: (IDataRow) => Option[Any] = getStructCreator(ctorm, arr, getMap, setMap)

    val getters = getfs.map(i => {
      val getCol: (Any) => Option[Any] = (row) => {
        val field = ScalaReflectionLock.synchronized {
          ReflectHelper.reflectField(row, i._1)
        }
        Option(i._2.convert(field.get))
      }
      val toRow: (Any) => Option[Any] = (row) => {
        val field = ScalaReflectionLock.synchronized {
          ReflectHelper.reflectField(row, i._1)
        }
        Option(i._2.toRow(field.get))
      }
      i._3 -> CF(i._3, i._2.colType, getCol, toRow)
    }).toMap
    val cfs = (arr.map(i => getters(i)) ++ getters.filterKeys(k => !arr.contains(k)).values).toArray
    CStructInfo(cfs, createStruct, r => new StructDataRow(r, cfs))
  }

  private def getStructCreator(ctorm: MethodMirror, arr: List[String],
                               getMap: Map[String, (TermSymbol, ColConvertor, String)],
                               setMap: Map[String, (TermSymbol, ColConvertor, String)]) = {
    val genStruct: (IDataRow) => Option[Any] = if (arr.isEmpty) (_) => Some(ctorm())
    else (row) =>
      if (row == null || row.isNull) None
      else {
        val args = arr.map(i => {
          val v = row.getVal(i)
          val k = i.toLowerCase
          if (v.isDefined && getMap.contains(k)) {
            getMap(k)._2.convert(v.get)
            //else if (setMap.contains(k)) setMap(k)._2.convert(v.get)
            //else v.get
          } else null
        })
        Some(ctorm(args: _*))
      }
    val createStruct: (IDataRow) => Option[Any] = row => {
      val v = genStruct(row)
      v match {
        case Some(x) => setMap.map(i => row.getVal(i._1).fold(None)(v => {
          val field = reflectField(x, i._2._1)
          val convertor = setMap(i._1.toLowerCase)
          field.set(convertor._2.convert(v))
          None
        }))
          Some(x)
        case _ => None
      }
    }
    createStruct
  }

  /**
    * createSchema and CStructInfo for T with synchronized lock
    *
    * @tparam T type
    * @return ([[DataSetSchema]], [[CStructInfo]])
    */
  def createSchema[T: TypeTag : ClassTag](): (DataSetSchema, CStructInfo) = ScalaReflectionLock.synchronized {
    val t = typeOf[T]
    val struct = createCStructInfo(t)
    val schema = new DataSetSchema(struct.elements.indices.map(i => {
      val field = struct.elements(i)
      val name = field.name
      val col = new DataColumn[T](i, name, field.element, r => {
        val v = field.getter(r)
        if (v.isDefined) Some(v.get.asInstanceOf[T]) else None
      })
      i -> col
    }).toMap)
    (schema, struct)
  }

  /**
    * convert field type tree to str or tuple that we can use
    *
    * @param tree field type tree
    * @return ColConvertor
    */
  def treeToStr(tree: Tree): Any = tree match {
    case Select(x, y) if y.decoded.startsWith("apply") => (treeToStr(x), y.decoded)
    case Apply(x, y) => (treeToStr(x), y.map(treeToStr))
    case Literal(Constant(x)) => x.toString
    case Ident(x) => x.decoded
    case Select(x) => x._2.decoded
    case TypeApply(x, y) => (treeToStr(x), y.map(treeToStr))
    case t: TypeTree => t.tpe
    case _ => "CNoSupport"
  }

  /**
    * get real name that we use
    *
    * @param name scala Reflect api
    * @return String
    */
  def getRealName(name: universe.Name): String = {
    name.decoded.trim
  }

  /**
    * get can read field and can write field  with synchronized lock
    *
    * @param t type
    * @return (Map[String, TermSymbol], Map[String, TermSymbol])
    */
  def getValAndVar(t: Type): (Map[String, TermSymbol], Map[String, TermSymbol]) = ScalaReflectionLock.synchronized {
    val fields = getFields(t).toArray
    val vals = fields.map(i => getRealName(i.name) -> i).toMap
    val vars = fields.filter(i => i.isVar).map(i => getRealName(i.name) -> i).toMap
    (vals, vars)
  }

  /**
    * reflect a Field with synchronized lock
    *
    * @param obj    origin obj data
    * @param member field reflect info
    * @tparam T obj Type
    * @return FieldMirror
    */
  def reflectField[T: ClassTag](obj: T, member: TermSymbol): FieldMirror = ScalaReflectionLock.synchronized {
    val m = mirror
    m.reflect(obj).reflectField(member)
  }
}
