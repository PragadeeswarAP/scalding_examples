package scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) {
      line : String => tokenize(line)
    }
    .groupBy('word ) { group:GroupBuilder => group.size }
    .filter('size){size : Int => size > 4}
    .write( Tsv( args("output") ) )

  // split a piece of text into individual words
  def tokenize(text : String) : List[String] = {
    List(text.toLowerCase.split("/")(2))
  }
}

object WordCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200","scalding.examples.WordCountJob",
    "--input", "/home/yooo/Documents/Indix/scalding/sampleurls",
    "--output", "/home/yooo/Documents/Indix/Scalding/output",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}