package org.example.taxi

import com.esri.core.geometry.{Geometry, GeometryEngine}
import org.json4s.JsonAST.JObject
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.jackson.Serialization

/*
  {
    "type": "FeatureCollection",
    "features": [
    {
      "type": "Feature",
      "id": 0,
      "properties": {
        "boroughCode": 5,
        "borough": "Staten Island",
        "@id": "http:\/\/nyc.pediacities.com\/Resource\/Borough\/Staten_Island"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
        [
        [-74.050508064032471, 40.566422034160816],
        [-74.049983525625748, 40.566395924928273]
        ]
        ]
      }
    }
    ]
  }
 */


case class FeatureCollection(features: List[Feature])

case class Feature(properties: Map[String, String], geometry: JObject){
  def getGeometry(): Geometry = {
    val mapGeo = GeometryEngine.geoJsonToGeometry(compact(render(geometry)), 0, Geometry.Type.Unknown)
    mapGeo.getGeometry
  }
}

object FeatureExtraction {

  def parseJson(json: String): FeatureCollection = {
    // 导入formats隐士转换
    implicit  val formats = Serialization.formats(NoTypeHints)
    // json-> Obj
    import org.json4s.jackson.Serialization.read
    val featureCollection = read[FeatureCollection](json)
    featureCollection
  }

}