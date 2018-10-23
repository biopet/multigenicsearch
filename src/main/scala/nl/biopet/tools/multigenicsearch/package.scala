package nl.biopet.tools

package object multigenicsearch {
  case class SingleVariant(contig: String, pos: Int, samples: List[Boolean])
  case class SingleVariantWithIndex(index: Long, variant: SingleVariant)
  case class SingleSamples(index: Long, samples: List[Boolean])
  case class Combination(indexes: List[Long], samples: List[List[Boolean]])
}
