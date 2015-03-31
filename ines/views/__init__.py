# -*- coding: utf-8 -*-

from ines.utils import format_error_to_json_values


def errors_json_view(context, request):
    values = format_error_to_json_values(context, request=request)
    return request.render_to_response(
        'json',
        values=values,
        status=values['status'])


class DefaultAPIView(object):
    def __init__(self, context, request):
        self.request = request
        self.context = context
        self.api = self.request.api

    def create_pagination(self, values_key, pagination, route_name, **params):
        result = {
            'page': pagination.page,
            'limit_per_page': pagination.limit_per_page,
            'last_page': pagination.last_page,
            'number_of_results': pagination.number_of_results,
            'number_of_page_results': pagination.number_of_page_results,
            values_key: pagination}
        result.update(self.create_pagination_href(route_name, pagination, **params))
        return result

    def create_pagination_href(self, route_name, pagination, **params):
        queries = {}
        for key, values in self.request.GET.dict_of_lists().items():
            values = [value for value in values if value]
            if values:
                queries[key] = values

        # Next page
        next_href = None
        next_page = pagination.page + 1
        if next_page <= pagination.last_page:
            queries['page'] = [next_page]
            next_href = self.request.route_url(
                route_name,
                _query=queries,
                **params)

        # Previous page
        previous_href = None
        previous_page = pagination.page - 1
        if previous_page >= 1:
            queries['page'] = [previous_page]
            previous_href = self.request.route_url(
                route_name,
                _query=queries,
                **params)

        # First page
        queries['page'] = [1]
        first_href = self.request.route_url(
            route_name,
            _query=queries,
            **params)

        # Last page
        queries['page'] = [pagination.last_page]
        last_href = self.request.route_url(
            route_name,
            _query=queries,
            **params)

        return {
            'next_page_href': next_href,
            'previous_page_href': previous_href,
            'first_page_href': first_href,
            'last_page_href': last_href}
