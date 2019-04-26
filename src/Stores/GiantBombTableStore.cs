using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool
{
    public sealed class GiantBombTableStore : IReadOnlyTableStore
    {
        private readonly WebClient _webClient;
        private readonly string _apiKey;
        private readonly bool _verbose;
        private DateTime _nextRequest = DateTime.MinValue;

        public GiantBombTableStore(string apiKey, string userAgent, bool verbose = false)
        {
            _webClient = new InternalWebClient(userAgent);
            _apiKey = apiKey;
            _verbose = verbose;
        }

        public IEnumerable<TableEntity> GetEntitiesById(string table, long nextId, TableConfig config)
        {
            // TODO: replace nextId with nextOffset
            long nextOffset = 0;
            bool finished = false;

            while (!finished)
            {
                var obj = DownloadResourceList(
                    table,
                    string.Join(",", config.Fields),
                    offset: nextOffset,
                    limit: 100,
                    sort: "id:asc");

                nextOffset = obj["offset"].Value<long>() + obj["number_of_page_results"].Value<long>();
                finished = obj["number_of_page_results"].Value<long>() < obj["limit"].Value<long>();

                foreach (var item in obj["results"])
                {
                    long id = item["id"].Value<long>();
                    var timestamp = DateTime.Parse(item["date_last_updated"].Value<string>().Replace(' ', 'T') + 'Z');
                    yield return new TableEntity(id, timestamp, (JObject)item);
                }
            }
        }

        private JObject DownloadResourceList(
            string resource,
            string fields,
            long offset,
            long? limit,
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
