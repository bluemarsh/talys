using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace Talys.Stores
{
    public sealed class IgdbTableStore : IReadOnlyTableStore
    {
        private readonly InternalWebClient _webClient;
        private readonly bool _verbose;
        private DateTime _nextRequest = DateTime.MinValue;

        public IgdbTableStore(string userAgent, string? apiKey = null, bool verbose = false)
        {
            _webClient = new InternalWebClient(userAgent, apiKey);
            _verbose = verbose;
        }

        public IEnumerable<TableEntity> GetEntitiesByTimestamp(
            string table,
            TableConfig config,
            DateTime? lastTimestamp,
            long? lastId)
        {
            const int limit = 50;

            SetApiKeyIfNeeded(config);

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
                // (this must also affect staging chunk file naming in local store -- since two files would have same timestamp)

                var uri = BuildListUri(
                    table,
                    string.Join(",", config.Fields),
                    limit: limit,
                    dateLastUpdated: lastTimestamp);
                var result = Download(uri);

                finished = result.Count <= limit;

                DateTime? firstTimestamp = null;

                foreach (var item in result.Items)
                {
                    var (id, timestamp) = ParseEntityProperties(item);

                    // TODO: handle multiple ids with the same timestamp (so we don't create staging file
                    // with only existing content, although it is handled correctly by upsert anyway)
                    if (id == lastId && timestamp == lastTimestamp)
                        continue; // don't add the last item again

                    if (firstTimestamp == null)
                        firstTimestamp = timestamp;

                    lastId = id;
                    lastTimestamp = timestamp;
                    yield return new TableEntity(id, timestamp, (JObject)item);
                }

                long offset = 0;
                while (firstTimestamp == lastTimestamp)
                {
                    // TODO: figure out how to avoid duplicating code from above

                    offset += 99;
                    //offset += 98; // 99 offset was acting like 100 when hit this for "people" table...

                    uri = BuildListUri(
                        table,
                        string.Join(",", config.Fields),
                        offset,
                        limit: limit,
                        dateLastUpdated: lastTimestamp);
                    result = Download(uri);

                    finished = result.Count <= limit;

                    firstTimestamp = null;

                    foreach (var item in result.Items)
                    {
                        var (id, timestamp) = ParseEntityProperties(item);

                        // First item should overlap with last item from previous download
                        if (id == lastId && timestamp == lastTimestamp)
                        {
                            continue; // don't add the last item again
                        }
                        else if (firstTimestamp == null)
                        {
                            // If needed, make this more robust to handle multiple ids and use lower offset
                            Console.WriteLine($"Expecting {lastId} but found {id}");
                            yield break;
                        }

                        if (firstTimestamp == null)
                            firstTimestamp = timestamp;

                        lastId = id;
                        lastTimestamp = timestamp;
                        yield return new TableEntity(id, timestamp, (JObject)item);
                    }
                }
            }
        }

        public TableEntity GetEntityDetail(string table, TableConfig config, long id)
        {
            throw new NotSupportedException();
        }

        public TableEntity GetEntityDetail(string table, TableConfig config, TableEntity entity)
        {
            throw new NotSupportedException();
        }

        private void SetApiKeyIfNeeded(TableConfig config)
        {
            if (string.IsNullOrEmpty(_webClient.ApiKey) && !string.IsNullOrEmpty(config.ApiKey))
                _webClient.ApiKey = config.ApiKey;
        }

        private static (long Id, DateTime Timestamp) ParseEntityProperties(JToken item)
        {
            long id = item["id"].Value<long>();
            var timestamp = DateTime.UnixEpoch.AddSeconds(item["updated_at"].Value<long>());
            return (id, timestamp);
        }

        private Uri BuildListUri(
            string resource,
            string fields,
            long offset = 0,
            long? limit = null,
            string? sort = null,
            DateTime? dateLastUpdated = null)
        {
            sort ??= "updated_at:asc";
            string s = $"https://api-v3.igdb.com/{resource}/?order={sort}";

            s += $"&fields={fields}";

            if (offset > 0)
                s += $"&offset={offset}";

            if (limit != null)
                s += $"&limit={limit}";

            if (dateLastUpdated != null)
                s += $"&filter[updated_at][gte]={dateLastUpdated.Value.Subtract(DateTime.UnixEpoch).TotalSeconds}";

            return new Uri(s);
        }

        /*private string BuildListQuery(
            string fields,
            long offset = 0,
            long? limit = null,
            string? sort = null,
            DateTime? dateLastUpdated = null)
        {
            sort ??= "updated_at:asc";
            string s = $"https://api-v3.igdb.com/{resource}/?order={sort}";

            s += $"&fields={fields}";

            if (offset > 0)
                s += $"&offset={offset}";

            if (limit != null)
                s += $"&limit={limit}";

            if (dateLastUpdated != null)
                s += $"&filter[updated_at][gte]={dateLastUpdated.Value.Subtract(DateTime.UnixEpoch).TotalSeconds}";
        }*/

        private (JArray Items, long Count) Download(Uri uri)
        {
            const int MaxRetries = 3;

            EnforceRateLimit();

            Trace.WriteLineIf(_verbose, string.Empty);
            Trace.WriteLineIf(_verbose, uri, typeof(IgdbTableStore).Name);

            string json;
            long count;
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    json = _webClient.DownloadString(uri);
                    count = Convert.ToInt64(_webClient.ResponseHeaders["X-Count"], CultureInfo.InvariantCulture);
                    break;
                }
                catch (WebException ex) when (attempt < MaxRetries)
                {
                    Console.WriteLine($"Retrying after failed {attempt + 1} time(s) with {ex.Status}: {ex.Message}");
                    EnforceRateLimit();
                    Thread.Sleep(TimeSpan.FromSeconds(Math.Pow(3, attempt)));
                }
            }

            var items = JArray.Parse(json);

            return (items, count);
        }

        private void EnforceRateLimit()
        {
            if (DateTime.UtcNow < _nextRequest)
                Thread.Sleep(_nextRequest - DateTime.UtcNow);
            _nextRequest = DateTime.UtcNow + TimeSpan.FromMilliseconds(1001);
        }

        private class InternalWebClient : WebClient
        {
            private readonly string _userAgent;

            internal InternalWebClient(string userAgent, string? apiKey)
            {
                _userAgent = userAgent;
                ApiKey = apiKey;
            }

            internal string? ApiKey { get; set; }

            protected override WebRequest GetWebRequest(Uri address)
            {
                var request = (HttpWebRequest)base.GetWebRequest(address);
                request.Accept = "application/json";
                request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
                request.Headers.Set(HttpRequestHeader.AcceptEncoding, "gzip, deflate");
                request.Headers.Set("user-key", ApiKey);
                //request.Headers.Set(HttpRequestHeader.AcceptLanguage, "en-US");
                request.UserAgent = _userAgent;
                return request;
            }
        }
    }
}
