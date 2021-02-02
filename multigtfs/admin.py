#
# Copyright 2012-2014 John Whitlock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import unicode_literals
from admin_confirm.admin import confirm_action
from django.core.exceptions import PermissionDenied

from django.contrib.gis import admin
from .tasks import delete_agency, delete_model
from admin_confirm import AdminConfirmMixin
from django.http import HttpResponseRedirect

from multigtfs.app_settings import MULTIGTFS_OSMADMIN
from multigtfs.models import (
    Agency, Block, Fare, FareRule, Feed, FeedInfo, Frequency, Route, Service,
    ServiceDate, Shape, ShapePoint, Stop, StopTime, Transfer, Trip, Zone)

geo_admin = admin.OSMGeoAdmin if MULTIGTFS_OSMADMIN else admin.GeoModelAdmin


class AgencyAdmin(AdminConfirmMixin, admin.ModelAdmin):
    list_display = ['name']
    raw_id_fields = ('feed', )
    actions = ['delete_model_agency']

    # used for confirmation message
    confirm_change = True
    confirmation_fields = []

    @confirm_action
    def delete_model_agency(self, request, queryset):
        [delete_agency.delay(q.id, q.name) for q in queryset]
        self.message_user(
            request, 'Deleting agency in progress, task may take a while')
        new_path = '/api/admin/django_celery_results/taskresult/?task_name=multigtfs.tasks.delete_agency'
        return HttpResponseRedirect(new_path)

    delete_model_agency.short_description = "Delete Model by Agency"
    delete_model_agency.allowed_permissions = ('change',)


class FeedAdmin(AdminConfirmMixin, admin.ModelAdmin):
    list_display = ['name']
    actions = ['delete_model']

    # used for confirmation message
    confirm_change = True
    confirmation_fields = []

    @confirm_action
    def delete_model(self, request):
        if not request.user.is_superuser:
            raise PermissionDenied

        delete_model.delay()
        self.message_user(
            request, 'Deleting entire model in progress, task may take a while !No Messing')
        new_path = '/api/admin/django_celery_results/taskresult/?task_name=gtfsApi.tasks.deleteGtfsModel'
        return HttpResponseRedirect(new_path)

    delete_model.short_description = "Delete entire gtfs model data !No Messing"
    delete_model.allowed_permissions = ('change',)


class BlockAdmin(admin.ModelAdmin):
    raw_id_fields = ('feed', )


class FareAdmin(admin.ModelAdmin):
    raw_id_fields = ('feed', )


class FareRuleAdmin(admin.ModelAdmin):
    raw_id_fields = ('fare', 'route', 'origin', 'destination', 'contains')


class FeedInfoAdmin(admin.ModelAdmin):
    raw_id_fields = ('feed', )


class FrequencyAdmin(admin.ModelAdmin):
    raw_id_fields = ('trip', )


class RouteAdmin(geo_admin):
    raw_id_fields = ('feed', 'agency')


class ServiceAdmin(admin.ModelAdmin):
    raw_id_fields = ('feed', )


class ServiceDateAdmin(admin.ModelAdmin):
    raw_id_fields = ('service', )


class ShapeAdmin(geo_admin):
    raw_id_fields = ('feed', )


class ShapePointAdmin(geo_admin):
    raw_id_fields = ('shape', )


class StopAdmin(geo_admin):
    raw_id_fields = ('feed', 'zone', 'parent_station')


class StopTimeAdmin(admin.ModelAdmin):
    raw_id_fields = ('stop', 'trip')


class TransferAdmin(admin.ModelAdmin):
    raw_id_fields = ('from_stop', 'to_stop')


class TripAdmin(geo_admin):
    raw_id_fields = ('route', 'service', 'block', 'shape')


class ZoneAdmin(admin.ModelAdmin):
    raw_id_fields = ('feed', )


admin.site.register(Agency, AgencyAdmin)
admin.site.register(Block, BlockAdmin)
admin.site.register(Fare, FareAdmin)
admin.site.register(FareRule, FareRuleAdmin)
admin.site.register(Feed, FeedAdmin)
admin.site.register(FeedInfo, FeedInfoAdmin)
admin.site.register(Frequency, FrequencyAdmin)
admin.site.register(Route, RouteAdmin)
admin.site.register(Service, ServiceAdmin)
admin.site.register(ServiceDate, ServiceDateAdmin)
admin.site.register(Shape, ShapeAdmin)
admin.site.register(ShapePoint, ShapePointAdmin)
admin.site.register(Stop, StopAdmin)
admin.site.register(StopTime, StopTimeAdmin)
admin.site.register(Transfer, TransferAdmin)
admin.site.register(Trip, TripAdmin)
admin.site.register(Zone, ZoneAdmin)
