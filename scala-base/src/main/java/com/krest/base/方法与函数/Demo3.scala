package com.krest.base.方法与函数

trait IsEquals {
  def isEqu(o: Any): Boolean

  def isNotEqu(o: Any): Boolean = {
    !isEqu(o)
  }
}


class Point(xx: Int, yy: Int) extends IsEquals {
  val x = xx
  val y = yy

  //  方法实现
  override def isEqu(o: Any): Boolean = {
    o.isInstanceOf[Point] && o.asInstanceOf[Point].x == this.x
  }
}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val p1 = new Point(11, 22)
    val p2 = new Point(11, 33)
    println(p1.isEqu(p2))
    println(p1.isNotEqu(p2))
  }
}
