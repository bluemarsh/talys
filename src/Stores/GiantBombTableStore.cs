﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool.Stores
{
    public sealed class GiantBombTableStore : IReadOnlyTableStore
    {
        private readonly WebClient _webClient;
        private readonly bool _verbose;
        private string? _apiKey;
        private DateTime _nextRequest = DateTime.MinValue;

        public GiantBombTableStore(string userAgent, string? apiKey = null, bool verbose = false)
        {
            _webClient = new InternalWebClient(userAgent);
            _apiKey = apiKey;
            _verbose = verbose;
        }

        public IEnumerable<TableEntity> GetEntitiesByTimestamp(
            string table,
            TableConfig config,
            DateTime? lastTimestamp,
            long? lastId)
        {
            if (string.IsNullOrEmpty(_apiKey) && !string.IsNullOrEmpty(config.ApiKey))
                _apiKey = config.ApiKey;

            bool finished = false;

            while (!finished)
            {
                string lastUpdateText = lastTimestamp != null ?
                    lastTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") :
                    "forever";
                Console.WriteLine($"Retrieving {table} updated since {lastUpdateText}");

                // TODO: we could hit an infinite loop here if there are more items with the same timestamp than the page limit
                // if this happened, would need to implement paging (with offset) to handle this case -- use offset of one less
                // than limit and ensure that the first item of next page matches the last item of the previous page

                var result = DownloadResourceList(
                    table,
                    string.Join(",", config.Fields),
                    sort: "date_last_updated:asc",
                    dateLastUpdated: lastTimestamp);

                finished = result["number_of_page_results"].Value<long>() == result["number_of_total_results"].Value<long>();

                foreach (var item in result["results"])
                {
                    var (id, timestamp) = ParseEntityProperties(item);

                    // TODO: handle multiple ids with the same timestamp (so we don't create staging file
                    // with only existing content, although it is handled correctly by upsert anyway)
                    if (id == lastId && timestamp == lastTimestamp)
                        continue; // don't add the last item again

                    lastId = id;
                    lastTimestamp = timestamp;
                    yield return new TableEntity(id, timestamp, (JObject)item);
                }
            }
        }

        public TableEntity GetEntityDetail(string table, TableConfig config, TableEntity entity)
        {
            if (string.IsNullOrEmpty(_apiKey) && !string.IsNullOrEmpty(config.ApiKey))
                _apiKey = config.ApiKey;

            string url = entity.Content["api_detail_url"].Value<string>();

            Console.WriteLine($"Retrieving {url}");

            var result = DownloadResourceDetail(
                url,
                string.Join(",", config.Fields.Concat(config.DetailFields)));
            var detail = (JObject)result["results"];

            var (id, timestamp) = ParseEntityProperties(detail);

            return new TableEntity(id, timestamp, detail);
        }

        private static (long Id, DateTime Timestamp) ParseEntityProperties(JToken item)
        {
            long id = item["id"].Value<long>();
            var timestamp = DateTime.Parse(
                item["date_last_updated"].Value<string>().Replace(' ', 'T') + 'Z',
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal);
            return (id, timestamp);
        }

        private JObject DownloadResourceList(
            string resource,
            string fields,
            long offset = 0,
            long? limit = null,
            string? sort = null,
            DateTime? dateLastUpdated = null)
        {
            var uri = BuildListUri(resource, fields, offset, limit, sort, dateLastUpdated);
            return DownloadCore(uri);
        }

        private JObject DownloadResourceDetail(string url, string fields)
        {
            var uri = BuildDetailUri(url, fields);
            return DownloadCore(uri);
        }

        private JObject DownloadCore(Uri uri)
        {
            EnforceRateLimit();

            Trace.WriteLineIf(_verbose, string.Empty);
            Trace.WriteLineIf(_verbose, uri, typeof(GiantBombTableStore).Name);

            var json = _webClient.DownloadString(uri);
            var obj = JObject.Parse(json);

            if (obj["status_code"].Value<int>() != 1)
                throw new Exception($"GiantBomb returned error {obj["status_code"]} '{obj["error"]}' for '{uri}'");

            return obj;
        }

        private void EnforceRateLimit()
        {
            // TODO: GiantBomb has new rate limits (200 requests per resource per hour), will need to account for this
            if (DateTime.UtcNow < _nextRequest)
                Thread.Sleep(_nextRequest - DateTime.UtcNow);
            _nextRequest = DateTime.UtcNow + TimeSpan.FromMilliseconds(1001);
        }

        private Uri BuildListUri(
            string resource,
            string fields,
            long offset,
            long? limit,
            string? sort,
            DateTime? dateLastUpdated)
        {
            sort ??= "date_last_updated:desc";
            string s = $"http://www.giantbomb.com/api/{resource}/?api_key={_apiKey}&format=json&sort={sort}";

            if (fields != null)
                s += $"&field_list={fields}";
            if (offset > 0)
                s += $"&offset={offset}";
            if (limit != null)
                s += $"&limit={limit}";
            if (dateLastUpdated != null)
                s += $"&filter=date_last_updated:{dateLastUpdated:yyyy-MM-dd HH:mm:ss}|{DateTime.UtcNow.AddYears(1):yyyy-MM-dd HH:mm:ss}";
            return new Uri(s);
        }

        private Uri BuildDetailUri(string url, string fields)
        {
            string s = $"{url}?api_key={_apiKey}&format=json";
            if (fields != null)
                s += $"&field_list={fields}";
            return new Uri(s);
        }

        private class InternalWebClient : WebClient
        {
            private readonly string _userAgent;

            internal InternalWebClient(string userAgent)
            {
                _userAgent = userAgent;
            }

            protected override WebRequest GetWebRequest(Uri address)
            {
                var request = (HttpWebRequest)base.GetWebRequest(address);
                request.Accept = "text/html, application/xhtml+xml, image/jxr, */*";
                request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
                request.Headers.Set(HttpRequestHeader.AcceptEncoding, "gzip, deflate");
                request.Headers.Set(HttpRequestHeader.AcceptLanguage, "en-US");
                request.UserAgent = _userAgent;
                return request;
            }
        }
    }
}
