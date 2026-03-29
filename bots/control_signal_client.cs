using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlgoSphere.Bots
{
    public class ControlSignal
    {
        public string name { get; set; } = "";
        public string control_state { get; set; } = "MONITOR";
        public bool control_active { get; set; } = true;
        public double effective_capital { get; set; } = 0.0;
        public string recommended_action { get; set; } = "NO_CHANGE";
    }

    public class ControlSignalsResponse
    {
        public int count { get; set; }
        public ControlSignal[] signals { get; set; } = Array.Empty<ControlSignal>();
    }

    /// <summary>
    /// Minimal read-only client for Algo Sphere control signals.
    /// </summary>
    public class ControlSignalClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;

        public ControlSignalClient(string apiBaseUrl = "http://127.0.0.1:8000")
            : this(new HttpClient(), apiBaseUrl)
        {
        }

        public ControlSignalClient(HttpClient httpClient, string apiBaseUrl = "http://127.0.0.1:8000")
        {
            _httpClient = httpClient;
            _apiBaseUrl = apiBaseUrl.TrimEnd('/');
        }

        public async Task<ControlSignal?> GetSignalForBotAsync(string botName)
        {
            var response = await _httpClient.GetAsync($"{_apiBaseUrl}/control/signals");
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            var json = await response.Content.ReadAsStringAsync();
            var payload = JsonSerializer.Deserialize<ControlSignalsResponse>(json);
            if (payload == null || payload.signals == null)
            {
                return null;
            }

            foreach (var signal in payload.signals)
            {
                if (string.Equals(signal.name, botName, StringComparison.Ordinal))
                {
                    return signal;
                }
            }
            return null;
        }
    }
}
