from django.contrib import admin
from django.http.response import HttpResponseRedirect
from .models import GtfsRApi
from django.shortcuts import render
from .tasks import download_realtime_data


class GtfsRApiAdmin(admin.ModelAdmin):
    list_display = ['timestamp']
    ordering = ['timestamp']
    actions = ['download_object']

    def download_object(self, request, queryset):
        # All requests here will actually be of type POST
        # so we will need to check for our special key 'apply'
        # rather than the actual request type
        if 'month' in request.POST or 'year' in request.POST:
            print(request.POST, 'wqeuqwerpqwioperiqwpoeiropquweiorupwieuropqwiuerpou')
            download_realtime_data(request.POST['year'], request.POST['month'])
            # @TODO finish this off

        if 'apply' in request.POST:
            # The user clicked submit on the intermediate form.
            # Perform our update action:
            # queryset.update(status='NEW_STATUS')

            # Redirect to our admin view after our update has
            # completed with a nice little info message saying
            # our models have been updated:
            # self.message_user(request,
            #                   "Changed status on {} orders".format(queryset.count()))
            return HttpResponseRedirect(request.get_full_path())

        return render(request,
                      'admin/gtfsRApi_intermediate.html',
                      context={'gtfsRApi': queryset})

    download_object.short_description = "Download selected objects"


admin.site.register(GtfsRApi, GtfsRApiAdmin)
