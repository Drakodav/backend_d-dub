from django.contrib import admin
from django.http.response import HttpResponse, HttpResponseRedirect
from .models import GtfsRApi
from django.shortcuts import render
from .tasks import download_realtime_data
from celery import current_app
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
        props = ['download', 'month', 'year']
        if all([p in request.POST for p in props]):
            records = None
            try:
                records = download_realtime_data(
                    request.POST['year'], request.POST['month'])
                # records = current_app.send_task(
                #     "download_realtime_data",
                #     args=(request.POST['year'], request.POST['month']),
                # )
            except download_realtime_data.OperationalError as exc:
                logger.exception('Sending task raised: %r', exc)

            filename = "gtfsRRecords.txt"
            response = HttpResponse(records, content_type='text/plain')
            response['Content-Disposition'] = 'attachment; filename={0}'.format(
                filename)
            # Redirect to our admin view after our update has
            # completed with a nice little info message
            self.message_user(request,
                              "Downloaded file")

            return response

        if 'back' in request.POST:
            return HttpResponseRedirect(request.get_full_path())

        return render(request, 'admin/gtfsRApi_intermediate.html', context={'selected_action': request.POST['_selected_action']})

    download_records.short_description = "Download monthly records"
    download_records.acts_on_all = True


admin.site.register(GtfsRApi, GtfsRApiAdmin)
