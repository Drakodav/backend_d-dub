import os
from celery.result import AsyncResult
from django.contrib import admin, messages
from django.http import FileResponse,  HttpResponseRedirect
from .models import GtfsRApi
from django.shortcuts import render
from gtfsRApi.tasks import download_realtime_data
from dynamoDub.settings import STATIC_ROOT


# from celery import current_app
from celery.utils.log import get_logger

logger = get_logger(__name__)


class GtfsRApiAdmin(admin.ModelAdmin):
    list_display = ['timestamp']
    ordering = ['-timestamp']
    actions = ['download_records']

    # https://stackoverflow.com/questions/4500924/django-admin-action-without-selecting-objects
    # allows for action to work without selection any object
    def changelist_view(self, request, extra_context=None):
        try:
            action = self.get_actions(request)[request.POST['action']][0]
            action_acts_on_all = action.acts_on_all
        except (KeyError, AttributeError):
            action_acts_on_all = False

        if action_acts_on_all:
            post = request.POST.copy()
            post.setlist(admin.helpers.ACTION_CHECKBOX_NAME,
                         self.model.objects.values_list('id', flat=True))
            request.POST = post

        return admin.ModelAdmin.changelist_view(self, request, extra_context)

    def download_records(self, request, queryset):
        context = {
            'selected_action': request.POST['_selected_action'],
        }

        if 'back' in request.POST:
            return HttpResponseRedirect(request.get_full_path())

        if all([p in request.POST for p in ['task_id', 'download']]) and request.POST['task_id']:
            result = AsyncResult(request.POST['task_id'])
            if result.get() == 'success':
                year, month = request.POST['year'], request.POST['month']

                source_name = "GtfsRRecords.zip"
                filepath = os.path.join(STATIC_ROOT, source_name)

                filename = "GtfsRRecords_{}-{}.zip".format(year, month)

                # Redirect to our admin view after our update has
                # completed with a nice little info message
                self.message_user(request, 'Download Successful')

                return FileResponse(open(filepath, 'rb'), content_type='application/zip', filename=filename, as_attachment=True)

            messages.error(request, 'Data is not available for this month')

        if all([p in request.POST for p in ['process', 'month', 'year']]):
            try:
                year, month = request.POST['year'], request.POST['month']
                # IMPORTANT, USE FULL MODULE PATH WHEN IMPORTING TASK
                result = download_realtime_data.delay(year, month)
                context['task_id'] = result.task_id

                return render(request, 'admin/gtfsRApi_intermediate.html', context=context)
            except download_realtime_data.OperationalError as exc:
                logger.exception('Sending task raised: %r', exc)

        return render(request, 'admin/gtfsRApi_intermediate.html', context=context)

    download_records.short_description = "Download monthly records"
    download_records.acts_on_all = True


admin.site.register(GtfsRApi, GtfsRApiAdmin)
