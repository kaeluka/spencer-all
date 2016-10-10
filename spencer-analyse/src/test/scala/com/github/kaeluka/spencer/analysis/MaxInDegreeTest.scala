package com.github.kaeluka.spencer.analysis

import org.hamcrest.core.IsEqual
import org.junit.Assert._
import org.junit.Test


object MaxInDegreeTest {
  def rng(s: Long, e: Long) : (Option[Long], Option[Long]) = (Some(s), Some(e))
  def until(end: Long) : (Option[Long], Option[Long]) = (None, Some(end))
  def from(start: Long) : (Option[Long], Option[Long]) = (Some(start), None)
}

class MaxInDegreeTest {
  @Test
  @throws[Exception]
  def highestOverlap(): Unit = {
    val r12 = (Some(1), Some(2))
    assertThat(MaxInDegree.highestOverlap(List()), new IsEqual(0))
    assertThat(MaxInDegree.highestOverlap(List(MaxInDegreeTest.rng(1, 2))), new IsEqual(1))
    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.rng(1, 2),
        MaxInDegreeTest.rng(1, 3))), new IsEqual(2))

    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.rng(1, 2),
        MaxInDegreeTest.rng(3, 4))), new IsEqual(1))

    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.rng(1, 3),
        MaxInDegreeTest.rng(2, 5),
        MaxInDegreeTest.rng(4, 6))), new IsEqual(2))

    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.rng(1, 3),
        MaxInDegreeTest.rng(2, 5),
        MaxInDegreeTest.rng(4, 6),
        MaxInDegreeTest.until(7))), new IsEqual(3))
//    (Some(1455698),None)
    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.rng(1, 4),
        MaxInDegreeTest.rng(1, 3),
        MaxInDegreeTest.from(2))), new IsEqual(3))

    assertThat(MaxInDegree.highestOverlap(
      List(
        MaxInDegreeTest.from(1455698))), new IsEqual(1))
  }
}