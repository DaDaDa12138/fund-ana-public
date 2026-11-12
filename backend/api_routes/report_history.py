"""Historical facts and labelled exports; never restore files during an HTTP read."""
from __future__ import annotations

from io import BytesIO
import sqlite3
from flask import jsonify, request, send_file, send_from_directory

from backend import report_history as history
from core.utils import report_history_store as store


def register_history_routes(app, settings):
    @app.get('/history')
    def history_page():
        return send_from_directory(settings.project_root/'frontend','history.html')

    @app.get('/report-history.js')
    def history_script():
        return send_from_directory(settings.project_root/'frontend','report-history.js')

    def error(exc):
        if isinstance(exc, FileNotFoundError):
            code = 503 if 'database_unavailable' in str(exc) else 404
        elif isinstance(exc, sqlite3.Error):
            code = 409 if getattr(exc,'sqlite_errorcode',0) & 255 in (sqlite3.SQLITE_CORRUPT,sqlite3.SQLITE_NOTADB) else 503
        elif 'database_unavailable' in str(exc): code = 503
        else: code = 409 if isinstance(exc, store.HistoryError) else 400
        return jsonify({'status': 'history_unavailable', 'error': str(exc), 'publication_truth': False}), code

    @app.get('/api/reports/history/<run_id>')
    def historical_report(run_id):
        try:
            value = history.get_report(settings, run_id, request.args.get('revision'))
            report = value['report']
            # The source report is retained intact; the public response advertises usable views.
            views = {}
            for name, key in [('formal','filtered_excel'),('tracker','project_tracker_excel'),
                              ('forward_validation','forward_validation_excel')]:
                raw = report.get('artifacts', {}).get(key)
                if not raw: continue
                rel = store.relative(settings.project_root, raw)
                source = next((s for s in value['sources'] if s['path']==rel and s['kind']=='workbook'),None)
                if source:
                    views[name] = {'original_status': __import__('backend.report_history_retention',fromlist=['original_state']).original_state(settings.project_root,settings.project_root/rel)['status'],
                                   'export_url': f'/api/reports/history/{run_id}/export?revision={value["revision"]}&view={name}&format=xlsx'}
            formal = next((s for s in value['sources'] if s['kind']=='formal'),None)
            formal_rows = None
            if formal:
                with store.connection(history.database(settings)) as conn:
                    frame = store.payload_frame(store.get_object(conn,formal['digest']))
                formal_rows = frame.astype(object).where(frame.notna(),None).to_dict(orient='records')
            requested_view=request.args.get('view','formal')
            if requested_view not in ('formal','tracker','forward_validation'):
                raise ValueError('history_view_invalid')
            sheets=history.historical_workbook(settings,value,requested_view)['sheets'] if requested_view!='formal' else None
            return jsonify({'formal_rows':formal_rows,'sheets':sheets,'view':requested_view,
                            'run_id':run_id,'revision':value['revision'],'semantic':value['semantic'],
                            'generated_at':report.get('generated_at'),'counts':report.get('counts'),
                            'nav_snapshot':(report.get('database_freshness') or {}).get('effective_nav_snapshot'),
                            'history_digest':value['history_digest'],'status':'available','views':views,
                            'as_of_original_publication':True,
                            'tracker_summary':(report.get('project_publish_summary') or {}).get('project_tracker_summary'),
                            'forward_validation_summary':report.get('forward_validation_summary'),
                            'publication_truth':value['semantic']=='committed'})
        except (ValueError, OSError, sqlite3.Error) as exc: return error(exc)

    @app.get('/api/reports/history/<run_id>/export')
    def historical_export(run_id):
        try:
            value=history.get_report(settings,run_id,request.args.get('revision'))
            fmt=request.args.get('format','xlsx');name=request.args.get('view','formal')
            raw=history.export_report(settings,value,name,fmt)
            response=send_file(BytesIO(raw),as_attachment=True,
                               download_name=f'{run_id}_{name}_regenerated.{fmt}',
                               mimetype='text/csv; charset=utf-8' if fmt=='csv' else
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response.headers['X-Fund-ANA-History-Digest']=value['history_digest']
            response.headers['X-Fund-ANA-Export-Semantics']='regenerated_original_as_of_values'
            return response
        except (ValueError, OSError, sqlite3.Error) as exc: return error(exc)
