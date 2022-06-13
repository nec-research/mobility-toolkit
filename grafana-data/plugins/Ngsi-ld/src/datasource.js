import _, { result } from "lodash";

export class GenericDatasource {

	constructor(instanceSettings, $q, backendSrv, templateSrv) {
		this.type = instanceSettings.type;
		this.url = instanceSettings.url;
		this.name = instanceSettings.name;
		this.q = $q;
		this.backendSrv = backendSrv;
		this.templateSrv = templateSrv;
		this.blub = instanceSettings.blub;
		this.withCredentials = instanceSettings.withCredentials;
		this.headers = { 'Content-Type': 'application/json' };
		if (typeof instanceSettings.basicAuth === 'string' && instanceSettings.basicAuth.length > 0) {
			this.headers['Authorization'] = instanceSettings.basicAuth;
		}
	}
	getMiddle(coordinates){
		var result = {};
		result.lat = 0;
		result.lng = 0;
		var count = 0;
		coordinates.forEach(function (entry) {
			count++;
			result.lat += entry[0];
			result.lng += entry[1];
		});
		result.lat = result.lat / count;
		result.lng = result.lng / count;
		return result;
	}
	placeInGrid(grid, temp, metricValue, gridValue){
		if(grid == undefined){
			return undefined;
		}
		var middle = this.getMiddle(temp.datapoints);
		var middleLat = middle.lat;
		var middleLng = middle.lng;
			
		for(var i = 0; i < grid.length; i++){
			var entry = grid[i];
			var coordinates = entry.datapoints;
			if(middleLat >= coordinates[0][0] && middleLng >= coordinates[0][1] && middleLat <= coordinates[2][0] && middleLng <= coordinates[2][1]){
				entry.count += 1;
				if(entry.count == 1){
					entry.target = (metricValue)
				}else{
					switch(gridValue){
						case 'average':
							entry.target = (entry.target + metricValue)/2;
							break;
						case 'min':
							if(entry.target > metricValue){
								entry.target = metricValue;
							}
							break;
						case 'max':
							if(entry.target < metricValue){
								entry.target = metricValue;
							}
							break;
						default:
							entry.target = (entry.target + metricValue)/2;
							break;
					}
				}
				grid[i] = entry;
				break;
			}
		}
		return grid;
	}
	getGrid(bounds) {
		var result = []
		var swlat = bounds._southWest.lat;
		var swlng = bounds._southWest.lng;
		var smallLat = swlat;
		var smallLng = swlng;
		var step = 10;
		var bigLat;
		var bigLng;
		var nelat = bounds._northEast.lat;
		var nelng = bounds._northEast.lng;
		if(smallLat < nelat){
			bigLat = nelat;
		}else{
			bigLat = smallLat;
			smallLat = nelat;
		}
		if(smallLng < nelng){
			bigLng = nelng;
		}else{
			bigLng = smallLng;
			smallLng = nelng;
		}
		var deltaLng = bigLng - smallLng;
		var deltaLat = bigLat - smallLat;
		var lngStep = deltaLng / step;
		var latStep = deltaLat / step;
		for(var i = 0; i < step; i++){
			for(var j = 0; j < step; j++){
				var temp = {};
				var coordinates = [];
				coordinates.push([smallLat + (i * latStep), smallLng + (j * lngStep)]);
				coordinates.push([smallLat + (i * latStep), smallLng + ((j+1) * lngStep)]);
				coordinates.push([smallLat + ((i+1) * latStep), smallLng + ((j+1) * lngStep)]);
				coordinates.push([smallLat + ((i+1) * latStep), smallLng + (j * lngStep)]);
				temp.datapoints = coordinates;
				temp.target = -1;
				temp.count = 0;
				result.push(temp);
			}
		}
		return result;
	}
	setParametersforQueryEditor(options) {    // Set Parameters from API written in the query editor
		var queryAPI = options.target;
		options.status = "Ok";
		if ((options.type == "table" || options.type == "geometry table") && queryAPI.indexOf("ngsi-ld/v1/entities") != -1) {
			if (queryAPI.indexOf("?") != -1) {
				var markIndex = queryAPI.indexOf("?");
				var typeIndex = queryAPI.indexOf("type");
				var parametersStr = queryAPI.slice(markIndex + 1, queryAPI.length);
				var parameterArr = parametersStr.split("&");

				for (var key in parameterArr) {
					var type = parameterArr[key].indexOf("type");
					var attr = parameterArr[key].indexOf("attrs");
					var id = parameterArr[key].indexOf("id");
					var idPattern = parameterArr[key].indexOf("idPattern");
					var equalToIndex = parameterArr[key].indexOf("=");
					var startIndex = equalToIndex + 1;
					var endIndex = parameterArr[key].length;
					var value;
					if (startIndex < endIndex) {
						value = parameterArr[key].slice(startIndex, endIndex);
					} else {
						options.status = "Error";
					}

					if (type != -1) {
						options.entityType = value;
					}
					if (attr != -1) {
						options.attribute = value;
					}
					if (id != -1) {
						options.entityId = value;
					}
				}
			}
		} else if ((options.type == "timeserie" || options.type == "geometry timeserie") && queryAPI.indexOf("ngsi-ld/v1/temporal/entities") != -1) {
			if (queryAPI.indexOf("?") != -1) {
				var markIndex = queryAPI.indexOf("?");
				var typeIndex = queryAPI.indexOf("type");
				var parametersStr = queryAPI.slice(markIndex + 1, queryAPI.length);
				var parameterArr = parametersStr.split("&");

				for (var key in parameterArr) {
					var timerel = parameterArr[key].indexOf("timerel");
					var time = parameterArr[key].indexOf("time");
					var endTime = parameterArr[key].indexOf("endTime");
					var type = parameterArr[key].indexOf("type");
					var attr = parameterArr[key].indexOf("attrs");
					var id = parameterArr[key].indexOf("id");
					var idPattern = parameterArr[key].indexOf("idPattern");
					var equalToIndex = parameterArr[key].indexOf("=");
					var startIndex = equalToIndex + 1;
					var endIndex = parameterArr[key].length;
					var value;
					if (startIndex < endIndex) {
						value = parameterArr[key].slice(startIndex, endIndex);
					} else {
						options.status = "Error";
					}
					if (type != -1) {
						options.entityType = value;
					}
					else if (attr != -1) {
						options.attribute = value;
					}
					else if (id != -1) {
						options.entityId = value;
					}
					else {
						options.additionalparams = "&" + parameterArr[key];
					}
				}
			}

		} else {
			options.status = "Error"
		}
		if (options.status == "Error") {
			options = "";
		}
		return options;
	}

	query(options) {

		if (!options.targets || options.targets.length <= 0 || !options.targets[0].entityType) {
			return this.q.when({ data: [] });
		}
		if (options.targets[0].rawQuery && options.targets[0].target != "") {
			var query = this.buildQueryParameters(options);
			options.targets[0] = this.setParametersforQueryEditor(query.targets[0]);
		}
		var resource;
		if (options.targets[0] == "") {
			alert("Please write the Query API in exact format");
			return this.q.when({ data: [] });
		}
		var mysettings = options.targets[0];

		if (mysettings.type === 'timeserie' || mysettings.type === 'geometry timeserie') {

			var fromDate = options.range.from.toISOString();
			var toDate = options.range.to.toISOString();
			resource = '/ngsi-ld/v1/temporal/entities?timerel=between&time=' + fromDate + '&endTime=' + toDate + '&type=' + encodeURIComponent(mysettings.entityType);
		} else if (mysettings.type === 'table' || mysettings.type === 'geometry table') {
			//table
			resource = '/ngsi-ld/v1/entities?type=' + encodeURIComponent(mysettings.entityType);
		}
		if (mysettings.entityId) {
			if (mysettings.entityId != '-1') {
				resource += '&id=' + encodeURIComponent(mysettings.entityId);
			}
		}

		if (mysettings.attribute) {
			if (mysettings.attribute != '-1') {
				resource += '&attrs=' + encodeURIComponent(mysettings.attribute);
			}
		}
		if (mysettings.additionalparams) {
			resource += mysettings.additionalparams;
		}

		var metric = "";
		var zoomValue = 0;
		if (options.scopedVars.__metric){
			metric = options.scopedVars.__metric.value;
		} else {
			metric = mysettings.metric
		}
		/*if (options.scopedVars.__zoom) {           //time series data handling for scope variables
			if (options.scopedVars.__metric) {
				metric = options.scopedVars.__metric.value;
				zoomValue = options.scopedVars.__zoom.value;
				resource += "&q=abstractionLevel==" + zoomValue;
			}
		} else if (options.scopedVars.__metric || options.scopedVars.__metric == "") {    // table data handling for scope variables
			if (options.scopedVars.__zoom) {
				metric = options.scopedVars.__metric.value;
				zoomValue = options.scopedVars.__zoom.value;
				resource += "&q=abstractionLevel==" + zoomValue;
			}
		} */
		if (options.scopedVars.__bounds) {
			var bounds = options.scopedVars.__bounds.value;
			var swlat = bounds._southWest.lat;
			var swlng = bounds._southWest.lng;
			var nelat = bounds._northEast.lat;
			var nelng = bounds._northEast.lng;
			resource += "&georel=intersects&geometry=Polygon&coordinates=[[[" + swlng + "," + swlat + "],[" + swlng + "," + nelat + "],[" + nelng + "," + nelat + "],[" + nelng + "," + swlat + "],[" + swlng + "," + swlat + "]]]"
		}
		if (metric!="" && (mysettings.type === 'table' || mysettings.type === 'geometry table')) {
				var fromDate = options.range.from.toISOString().split(".")[0] + "Z";
				var toDate = options.range.to.toISOString().split(".")[0] + "Z";
				resource += "&q="+metric+".observedAt>="+fromDate+";"+metric+".observedAt<="+toDate;
		}
		var ldquery = this.url + resource;
		var result;
		var headers = {};
		if (mysettings.atcontext) {
			headers.Link = '<' + mysettings.atcontext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"';
		}
		if (mysettings.type == 'timeserie') {
			var timeseriesScope = this;
			result = this.doRequest({
				url: ldquery,
				method: 'GET',
				headers: headers
			}).then(function (response) {
				return timeseriesScope.getTimeSeries(response, mysettings.countryKey);
			});
		} else if (mysettings.type == 'geometry table') {
			var tableScope = this;
			result = this.doRequest({
				url: ldquery,
				method: 'GET',
				headers: headers
			}).then(function (response) {
				return tableScope.getGeometryTableData(response, metric, options);
			});

		} else if (mysettings.type == 'geometry timeserie') {
			var timeseriesScope = this;
			result = this.doRequest({
				url: ldquery,
				method: 'GET',
				headers: headers
			}).then(function (response) {
				return timeseriesScope.getGeometryTimeSeries(response, mysettings.countryKey, metric);
			});

		} else {
			result = this.doRequest({
				url: ldquery,
				method: 'GET',
				headers: headers
			}).then(this.getTable);
		}

		return result;

	}
	finalizeGrid(grid){
		for(var i = 0; i < grid.length; i++){
			var item = grid[i];
			item.target = item.target + "&&" + item.target;
			grid[i] = item;
		}
		return grid;
	}
	getGeometryTableData(data, metric, options) {
		var dataResult = [];
		var mydata = data.data;
		var grid = undefined;
		var gridValue = 'average';
		if(options.scopedVars.__bounds && options.scopedVars.__zoom && options.scopedVars.__zoom.value < 16) {
			grid = this.getGrid(options.scopedVars.__bounds.value);
			gridValue = options.targets[0].gridValue;
		}
		
		var mySelf = this;
		mydata.forEach(function (entry) {
			var temp = {};
			var allInfos = "";
			var allInfosName = "@@@allinfos@@@";
			var metricValue = "";
			
			for (let [key, value] of Object.entries(entry)) {
				if (key == 'location' || value.type == 'GeoProperty') {
					if (value.value.type == "Polygon") {
						var coordinates = [];
						value.value.coordinates[0].forEach(function (entry2) {
							coordinates.push([entry2[1], entry2[0]])
						});
						temp.datapoints = coordinates;

					} else if (value.value.type == "Point") {
						var arr = [];
						arr.push([value.value.coordinates[1], value.value.coordinates[0]]);
						temp.datapoints = arr;
					}
				}
				else {
					if (key == metric) {
						metricValue = value.value;
					}
					var entryValue;
					if (value.value != undefined) {
						entryValue = value.value;
					} else if (key == 'belongsTo' && value.object) {
						entryValue = value.object;
					}
					else {
						entryValue = value;
					}
					allInfos += "<b>" + key + "</b>:   " + JSON.stringify(entryValue, null, 2) + "<br>";
				}
				temp.target = allInfos + "&&" + metricValue;

			}
			grid = mySelf.placeInGrid(grid, temp, metricValue, gridValue);
			dataResult.push(temp)
		});  
		if(grid != undefined){
			grid = this.finalizeGrid(grid);
			if(options.scopedVars.__zoom && options.scopedVars.__zoom.value > 14){
				dataResult = dataResult.concat(grid);
			}else{
				dataResult = grid;
			}
		}
		
		var result = { data: dataResult };
		return result;
	}
	getGeometryTimeSeries(data, countryKey, metric) {
		var mydata = data.data;
		var result = [];
		if (Object.keys(mydata).length == 0) {
			mydata = [];
		} else if (Object.keys(mydata).length != 0 && !Array.isArray(mydata)) {
			mydata = [mydata];
		}
		mydata.forEach(function (entry) {
			var temp = {};
			var countryExist = false;
			var allInfos = "";
			var metricValue = "";

			for (let [key, value] of Object.entries(entry)) {
				var entryValue = "";
				if (key != "location") {
					if (Array.isArray(value)) {
						if (key == 'belongsTo') {
							entryValue = value[0].object;
						} else {
							entryValue = value[0].value;
						}
					} else if (value.value != undefined) {
						if (key == 'belongsTo') {
							entryValue = value.object;
						} else {
							entryValue = value.value;
						}
					} else {
						entryValue = value;
					}

					allInfos += "<b>" + key + "</b>:   " + JSON.stringify(entryValue, null, 2) + "<br>";
				}
				if (key == countryKey) {
					countryExist = true;
				}
				if (countryExist && key == metric) {
					countryExist = true;
					var noiseLevel = value;
					if (Array.isArray(value)) {
						metricValue = noiseLevel[0].value;
					} else {
						metricValue = noiseLevel.value;
					}
				}
				if (countryExist && key == 'location') {
					var locationValue = value;
					var arr = [];
					if (Array.isArray(locationValue)) {
						if (locationValue[0].value.type == "Point") {
							arr.push(locationValue[0].value.coordinates);
							temp.datapoints = arr;
						} else if (locationValue[0].value.type == "Polygon") {
							var coordinates = [];
							locationValue[0].value.coordinates[0].forEach(function (entry2) {
								coordinates.push([entry2[1], entry2[0]])
							});
							temp.datapoints = coordinates;

						}
					}
					else {
						if (locationValue.value.type == "Point") {
							arr.push(locationValue.value.coordinates);
							temp.datapoints = arr;
						} else if (locationValue.value.type == "Polygon") {
							var coordinates = [];
							locationValue.value.coordinates[0].forEach(function (entry2) {
								coordinates.push([entry2[1], entry2[0]])
							});
							temp.datapoints = coordinates;

						}
					}
					temp.target = allInfos + "&&" + metricValue;
					result.push(temp);
				}
			}
		});
		return { data: result };
	}

	getTable(data) {
		var mydata = data.data;
		var temp = {};
		temp.type = "table";
		temp.rows = [];
		temp.columns = [];

		mydata.forEach(function (entry) {
			var row = [];
			var allInfos = "";
			var allInfosName = "@@@allinfos@@@";
			var locType = "";
			if (entry.hasOwnProperty('location')) {
				locType = entry['location'].value.type;
			}


			for (let [key, value] of Object.entries(entry)) {
				if (locType == "Point") {
					if (key == 'location' || (value.type && value.type == 'GeoProperty')) {

						var coordinates = value.value.coordinates;
						while (Array.isArray(coordinates[0])) {
							coordinates = coordinates[0];
						}
						var index = temp.columns.map(function (e) { return e.text; }).indexOf("longitude");
						if (index == -1) {
							index = temp.columns.length;
							temp.columns.push({ text: "longitude" });
						}
						row[index] = coordinates[0];

						index = temp.columns.map(function (e) { return e.text; }).indexOf("latitude");
						if (index == -1) {
							index = temp.columns.length;
							temp.columns.push({ text: "latitude" });
						}
						row[index] = coordinates[1];

					} else {

						var index = temp.columns.map(function (e) { return e.text; }).indexOf(key);
						if (index == -1) {
							index = temp.columns.length;
							temp.columns.push({ text: key });
						}
						if (value.value) {
							row[index] = value.value;
						} else {
							row[index] = value;
						}
						var entryValue;
						if (value.value != undefined) {
							entryValue = value.value;
						} else if (key == 'belongsTo' && value.object) {
							entryValue = value.object;
						}
						else {
							entryValue = value;
						}
						allInfos += "<b>" + key + "</b>:   " + JSON.stringify(entryValue, null, 2) + "<br>";
					}
				}
			}
			if (row.length > 0) {
				var index = temp.columns.map(function (e) { return e.text; }).indexOf(allInfosName);
				if (index == -1) {
					index = temp.columns.length;
					temp.columns.push({ text: allInfosName });
				}
				allInfos += "<b>metric:</b>";
				row[index] = allInfos;
				temp.rows.push(row);
			}
		});
		var result = [temp];
		return { data: result };
	}
	getTimeSeries(data, countryKey) {
		var mydata = data.data;
		var result = [];

		if (!Array.isArray(mydata)) {
			mydata = [mydata];
		}
		var values = [];

		mydata.forEach(function (entry) {
			var temp = {};
			temp.datapoints = [];
			var countryExist = false;
			for (let [key, value] of Object.entries(entry)) {

				if (key == 'type' || key == 'id' || key == '@context' || key == 'createdAt' || key == 'observedAt') {
					continue;
				}

				if (key == countryKey) {
					countryExist = true;
					var country = value;
					if (Array.isArray(country)) {
						temp.target = country[0].value;
					} else {
						temp.target = country.value;
					}
				}
				if (countryExist && key == 'noiselevel') {
					var tempEntry = [];
					if (Array.isArray(value)) {
						for (var index in value) {
							tempEntry = [];
							tempEntry[0] = parseFloat(value[index].value);
							tempEntry[1] = new Date(value[index].observedAt).getTime();
							temp.datapoints.push(tempEntry);
						}
					} else {
						tempEntry[0] = parseFloat(value.value);
						tempEntry[1] = new Date(value.observedAt).getTime();
						temp.datapoints.push(tempEntry);
					}
					result.push(temp);
					break;
				}
			}
		});

		return { data: result };
	}
	testDatasource() {
		return this.doRequest({
			url: this.url + '/scorpio/v1/info/types',
			method: 'GET',
		}).then(response => {
			if (response.status === 202) {
				return { status: "success", message: "Data source is working", title: "Success" };
			}
		});
	}

	annotationQuery(options) {
		var query = this.buildQueryParameters(options);
		query.targets = query.targets.filter(t => !t.hide);
		var resource = 'entities';
		if (options.requesttype === 'history') {
			resource = 'temporal';
		}

		var type = options.type;
		var location = query.location;
		if (query.targets.length <= 0) {
			return this.q.when({ data: [] });
		}

		if (this.templateSrv.getAdhocFilters) {
			query.adhocFilters = this.templateSrv.getAdhocFilters(this.name);
		} else {
			query.adhocFilters = [];
		}
		var ldquery = this.url + '/ngsi-ld/v1/entities/?type=' + type;
		if (location.length > 0) {
			ldquery = ldquery + '&' + location;
		}
		return this.doRequest({
			url: ldquery,
			method: 'GET'
		});

	}

	metricFindQuery(query) {
		var queryType = query.type || 'type';

		var result;
		var headers = {};
		if (query.atcontext) {
			headers['Link'] = '<' + query.atcontext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"';
		}

		switch (queryType) {
			case 'type':
				result = this.doRequest({
					url: this.url + '/scorpio/v1/info/types/',
					method: 'GET'
				}).then(this.mapToTextValue);
				break;
			case 'id':
				if (!query.entityType || query.entityType == 'Select an entity type first') {
					result = this.mapToTextValue({ data: ['Select an entity type first'] });
				} else {
					var result = this.doRequest({
						url: this.url + '/ngsi-ld/v1/entities?type=' + encodeURIComponent(query.entityType),
						method: 'GET',
						headers: headers
					}).then(this.getIdsFromQuery);
				}
				break;
			case 'attribs':
				if (!query.entityType || query.entityType == 'Select an entity type first') {
					result = this.mapToTextValue({ data: ['Select an entity type first'] });
				} else {
					var queryUrl = this.url + '/ngsi-ld/v1/entities?type=' + encodeURIComponent(query.entityType);

					if (query.entityId && query.entityId != 'Select an entity type first') {
						queryUrl = queryUrl + '&id=' + encodeURIComponent(query.entityId);
					}
					result = this.doRequest({
						url: queryUrl,
						method: 'GET',
						headers: headers
					}).then(this.getAttribsFromQuery);
				}
				break;
			default:
				result = null;
				break;
		}

		return result;

	}
	mapArrayToTextValue(data) {
		var myresult = [];

		data.forEach(function (entry) {
			myresult.push({ text: entry, value: entry });
		});
		return myresult;

	}
	getAttribsFromQuery(queryResult) {

		var data = queryResult.data;
		var result = [];
		result.push({ text: 'All', value: '-1' });
		data.forEach(function (entry) {
			for (let [key, value] of Object.entries(entry)) {
				if (key == 'type' || key == 'id' || key == '@context') {
					continue;
				}
				var index = result.map(function (e) { return e.text; }).indexOf(key);

				if (index == -1) {
					result.push({ text: key, value: key });
				}
			}
		});

		return result;
	}
	getIdsFromQuery(queryResult) {
		var result = [];
		result.push({ text: 'All', value: '-1' });
		var data = queryResult.data;
		if (Array.isArray(data)) {
			data.forEach(function (entry) {
				var id = entry.id;
				result.push({ text: id, value: id });
			});
		} else {
			var id = data.id;
			result.push({ text: id, value: id });
		}

		return result;
	}
	getIdFromEntity(entity) {
		return entity.id;
	}
	mapToTextValue(result) {

		return _.map(result.data, (d, i) => {
			if (d && d.text && d.value) {
				return { text: d.text, value: d.value };
			} else if (_.isObject(d)) {
				return { text: d, value: i };
			}
			return { text: d, value: d };
		});
	}

	doRequest(options) {
		options.withCredentials = this.withCredentials;
		options.headers = this.headers;
		return this.backendSrv.datasourceRequest(options);
	}

	buildQueryParameters(options) {
		//remove placeholder targets
		options.targets = _.filter(options.targets, target => {
			return target.target !== 'Enter Query API';
		});

		var targets = _.map(options.targets, target => {
			return {
				target: this.templateSrv.replace(target.target, options.scopedVars, 'regex'),
				refId: target.refId,
				hide: target.hide,
				type: target.type || 'timeserie',
				countryKey: (target.type == 'timeserie' || target.type == 'geometry timeserie') ? target.countryKey : ""
			};
		});

		options.targets = targets;

		return options;
	}

	getTagKeys(options) {
		return new Promise((resolve, reject) => {
			this.doRequest({
				url: this.url + '/tag-keys',
				method: 'POST',
				data: options
			}).then(result => {
				return resolve(result.data);
			});
		});
	}

	getTagValues(options) {
		return new Promise((resolve, reject) => {
			this.doRequest({
				url: this.url + '/tag-values',
				method: 'POST',
				data: options
			}).then(result => {
				return resolve(result.data);
			});
		});
	}

}
