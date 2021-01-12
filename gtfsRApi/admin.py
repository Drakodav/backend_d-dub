from django.contrib import admin
from django.http.response import HttpResponse, HttpResponseRedirect
from .models import GtfsRApi
from django.shortcuts import render
from .tasks import download_realtime_data


class GtfsRApiAdmin(admin.ModelAdmin):
    list_display = ['timestamp']
    ordering = ['timestamp']
    actions = ['download_records']

    def download_records(self, request, queryset):
        props = ['download', 'month', 'year']
        if all([p in request.POST for p in props]):
            records = download_realtime_data(
                request.POST['year'], request.POST['month'])

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


admin.site.register(GtfsRApi, GtfsRApiAdmin)
