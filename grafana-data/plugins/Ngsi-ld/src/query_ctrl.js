import { QueryCtrl } from 'app/plugins/sdk';
import _ from 'lodash'

export class GenericDatasourceQueryCtrl extends QueryCtrl {

	constructor($scope, $injector, uiSegmentSrv) {
		super($scope, $injector);
		this.scope = $scope;
		this.entityTypeSegment = this.target.entityType || 'Select an entity type first';
		this.entityIdSegment = this.target.entityId || 'Select an entity type first';
		this.target.type = this.target.type || 'timeserie';
		this.target.gridValue = this.target.gridValue || 'average';
		this.target.metric = this.target.metric || '';
		this.gridValueSegment = this.target.gridValue || 'average';
	}

	toggleEditorMode() {
		this.target.rawQuery = !this.target.rawQuery;
	}

	onChangeInternal() {
		this.panelCtrl.refresh(); // Asks the panel to refresh data.
	}

	onChangeCountryKey() {  // On country change refresh the panel
		this.onChangeInternal();
	}

	getEntityTypes() {
		var query = {};
		query.type = 'type';
		if (this.target.atcontext) {
			query.atcontext = this.target.atcontext;
		}
		return this.datasource.metricFindQuery(query);
	}

	typesChanged() {
		this.target.entityType = this.entityTypeSegment;
		this.onChangeInternal();
	}
	getGridSettings(){
		return '<li data-value="average"><a href="#">average</a></li><li data-value="min"><a href="#">min</a></li><li data-value="max"><a href="#">max</a></li>';
	}
	gridChanged(){
		this.target.gridValue = this.gridValueSegment;
		this.onChangeInternal();
	}
	idsChanged() {
		this.target.entityId = this.entityIdSegment;
		this.onChangeInternal();
	}
	getEntityIds() {
		var query = {};
		query.type = 'id';
		query.entityType = this.target.entityType;
		if (this.target.atcontext) {
			query.atcontext = this.target.atcontext;
		}

		return this.datasource.metricFindQuery(query);
	}

}

GenericDatasourceQueryCtrl.templateUrl = 'partials/query.editor.html';

