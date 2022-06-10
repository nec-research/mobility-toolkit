import requests
import json
import random
import schedule
import time
import datetime
import os
from threading import Thread
import dash_leaflet as dl
import dash_leaflet.express as dlx
import pandas as pd
from dash_extensions.javascript import assign
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

class NGSITools:
  coreContext = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld'
  def __init__(self):
    self.myThread = MyThread()
    self.myThread.start()


  def getAtContext(self, atContext):
    if(atContext == None):
      atContext = self.coreContext

  def getAllTypes(self, host, atContext):
    atContext = self.getAtContext(atContext)
    response = requests.get(host + '/ngsi-ld/v1/types', headers = {'Link':  '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'})
    return response.json()
    #return json
  def startPolling(self, host, atContext, entityType, limit, pollTime, callback):
    self.myThread.schedule(host, atContext, entityType, limit, pollTime, callback)

  def startPollingHistory(self, host, atContext, entityType, limit, fromDate, toDate, pollTime, callback):
    self.myThread.scheduleHistory(host, atContext, entityType, limit, fromDate, toDate, pollTime, callback)

  def getAttributesForType(self, host, atContext, entityType):
    atContext = self.getAtContext(atContext)
    response = requests.get(host + '/ngsi-ld/v1/types/' + entityType, headers = {'Link':  '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'})
    jsonResult = response.json()
    return jsonResult['attributeDetails']



class MyThread(Thread):

  def getTemporalEntities(host, atContext, entityType, limit, fromDate, toDate, callback):
    response = requests.get(host + '/ngsi-ld/v1/temporal/entities?type=' + entityType + '&limit=' + limit + '&timrel=between&timeAt='+fromDate+'&endTimeAt='+toDate, headers = {'Link':  '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"', 'Accept': 'application/geo+json'})
    jsonR = response.json()
    callback(jsonR, entityType)

  def getEntities(host, atContext, entityType, limit, callback):
    response = requests.get(host + '/ngsi-ld/v1/entities?type=' + entityType + '&limit=' + limit, headers = {'Link':  '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"', 'Accept': 'application/geo+json'})
    jsonR = response.json()
    callback(jsonR, entityType)

  def schedule(self, host, atContext, entityType, limit, pollTime, callback):
    schedule.clear()
    schedule.every(pollTime).seconds().do(getEntities, host=host, atContext=atContext, entityType=entityType, limit=limit, callback=callback)

  def scheduleHistory(self, host, atContext, entityType, limit, fromDate, toDate, pollTime, callback):
    schedule.clear()
    schedule.every(pollTime).seconds().do(getTemporalEntities, host=host, atContext=atContext, entityType=entityType, limit=limit, fromDate=fromDate, toDate=toDate, callback=callback)
  def run(self):
    while True:
      schedule.run_pending()
      time.sleep(1)


def loadType2Attribs():
  splitted = defaultEntityTypeAttributeCombos.split(',')
  result = {}
  for entry in splitted:
    tmp = entry.split(';')
    if(tmp[0] not in result):
      result[tmp[0]] = set()
    result[tmp[0]].add(tmp[1])
  return result

def getColorScales():
  tmp = os.getenv('SCALES', 'green;yellow;orange;red;red,blue;purple;brown,white;grey;black').split(',')
  result = []
  for entry in tmp:
    result.append(entry.split(';'))
  return result

defaultPollTime=os.getenv('POLL_TIME', 5)
defaultHost=os.getenv('DEFAULT_HOST', 'http://192.168.42.226:9090')
defaultLimit=os.getenv('DEFAULT_LIMIT', 1000)
defaultAtContext=os.getenv('DEFAULT_AT_CONTEXT', 'https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld')
defaultEntityTypeAttributeCombos=os.getenv('DEFAULT_TYPE_ATTRS_COMBOS', 'EmissionObserved;co2')
defaultRange=os.getenv('DEFAULT_RANGE', 3600*24)
defaultMins=os.getenv('MINSCALES', '0,0').split(',')
defaultMaxs=os.getenv('MAXSCALES', '50,100').split(',')
defaultScaleUnits=os.getenv('SCALEUNITS', 'g,m/s^2').split(',')

colorScales=getColorScales()




chroma = "https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js"  # js lib used for colors
type2Attribs = loadType2Attribs()

def getToolTip(entityId, properties):
  result = "<b>" + entityId + "</b><br><b>" + properties['type'] + "</b><br>"
  del properties['type']
  del properties['location']
  for key, value in properties.items():
    result = result + "<b>" + key + ":</b>"
    if value['type'] == "Relationship":
      result = result + value['object']
    else:
      result = result + str(value['value'])
    result = result + "<br>"
  return result

def clearGeoJson(entities, attrib):
  if 'features' not in entities:
    return None
  features = entities['features']
  for feature in features:
    entityId = feature['id']
    tooltip = getToolTip(entityId, feature['properties'])
    value = feature['properties'][attrib]['value']
    feature['properties'] = {attrib: value, 'tooltip': tooltip}
  print(json.dumps(entities, indent=4))
  return entities
def leafCallback(n, entityTypeAttrib):
  splitted = entityTypeAttrib.split(';')
  global defaultRange  
  date = datetime.datetime.fromtimestamp(time.time() - defaultRange).strftime('%Y-%m-%dT%H:%M:%SZ')
  print(defaultHost + '/ngsi-ld/v1/entities?type='+splitted[0]+'&limit=' + str(defaultLimit) + '&q=' + splitted[1] + '.observedAt>=' + date)
  entities = requests.get(defaultHost + '/ngsi-ld/v1/entities?type='+splitted[0]+'&limit=' + str(defaultLimit) + '&q=' + splitted[1] + '.observedAt>=' + date, headers = {'Link':  '<' + defaultAtContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"', 'Accept': 'application/geo+json'}).json()
  clearedGeoJson = clearGeoJson(entities, splitted[1])
  if clearedGeoJson==None:
    return None
  geobuf = dlx.geojson_to_geobuf(clearedGeoJson)
  return geobuf

def getMinMax(clearedGeoJson, attrib):
  features = clearedGeoJson['features']
  maxR = -999999999999999999999
  minR = 999999999999999999999
  for feature in features:
    value = feature['properties'][attrib]
    if(value < minR):
      minR = value
    if(value > maxR):
      maxR = value
  return [minR, maxR]

def initialMapSetup(app, entities, entityType):
  global type2Attribs
  global colorScales
  attribs = type2Attribs[entityType]
  count = 0
  mapLayers = []
  for attrib in attribs:
    clearedGeoJson = clearGeoJson(entities, attrib)
    if clearedGeoJson==None:
      continue
    minMax = [int(defaultMins[count]), int(defaultMaxs[count])]
    
    geobuf = dlx.geojson_to_geobuf(clearedGeoJson)
    colorscale = colorScales[count]
    colorbar = dl.Colorbar(colorscale=colorscale, width=20, height=150, min=minMax[0], max=minMax[1], unit=defaultScaleUnits[count])
    count = count + 1
    # Geojson rendering logic, must be JavaScript as it is executed in clientside.
    point_to_layer = assign("""function(feature, latlng, context){
        const {min, max, colorscale, circleOptions, colorProp} = context.props.hideout;
        const csc = chroma.scale(colorscale).domain([min, max]).mode('lch');  // chroma lib to construct colorscale
        circleOptions.fillColor = csc(feature.properties[colorProp]);  // set color based on color prop.
        return L.circleMarker(latlng, circleOptions);  // sender a simple circle marker.
    }""")
    # Create geojson.
    geojson = dl.GeoJSON(data=geobuf, id=entityType+";"+attrib, format="geobuf",
                     zoomToBounds=False,  # when true, zooms to bounds when data changes
                     options=dict(pointToLayer=point_to_layer),  # how to draw points
                     superClusterOptions=dict(radius=50),   # adjust cluster size
                     hideout=dict(colorProp=attrib, circleOptions=dict(fillOpacity=1, stroke=False, radius=5),
                                  min=minMax[0], max=minMax[1], colorscale=colorscale))
    mapLayers.append(geojson)
    mapLayers.append(colorbar)
  return [mapLayers, {'output': [entityType+";"+attrib, "data"],'input': [['interval-component', 'n_intervals'], [entityType+";"+attrib, 'id']]}]

def initialSetup(app):
  global type2Attribs
  mapLayers = []
  callbackTuples = []
  for entityType in type2Attribs.keys():
    mapSet = initialMapSetup(app, requests.get(defaultHost + '/ngsi-ld/v1/entities?type=' + entityType + '&limit=' + str(defaultLimit), headers = {'Link':  '<' + defaultAtContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"', 'Accept': 'application/geo+json'}).json(), entityType);
    mapLayers = mapLayers + mapSet[0]
    callbackTuples.append(mapSet[1])
  mapLayers.append(dl.TileLayer())
  interval = dcc.Interval(
            id='interval-component',
            interval=defaultPollTime*1000, # in milliseconds
            n_intervals=0
        )
  app.layout = html.Div([
    dl.Map(mapLayers, style={'width': '100%', 'height': '50vh', 'margin': "auto", "display": "block", "position": "relative"}, id='map'), interval, html.Div(id="capital")])
  for callbackTuple in callbackTuples:
    app.callback(Output(callbackTuple['output'][0], callbackTuple['output'][1]), [Input(callbackTuple['input'][0][0], callbackTuple['input'][0][1]),Input(callbackTuple['input'][1][0], callbackTuple['input'][1][1])])(leafCallback)
# Create the app.
app = Dash(external_scripts=[chroma], prevent_initial_callbacks=True)
initialSetup(app)




if __name__ == '__main__':
    app.run_server()


