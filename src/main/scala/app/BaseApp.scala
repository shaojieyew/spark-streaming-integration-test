package app

class BaseApp {

  def main(args: Array[String]): Unit = {
    preprocess(args)
    process()
    postprocess()
  }

  def preprocess(args: Array[String]) = {}

  def process() = {}

  def postprocess() = {}
}