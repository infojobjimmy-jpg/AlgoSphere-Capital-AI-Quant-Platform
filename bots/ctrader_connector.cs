using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlgoSphere.Bots
{
    /// <summary>
    /// Minimal connector for posting bot updates to local FastAPI.
    /// This is a simple baseline that can be replaced by a real cTrader integration.
    /// </summary>
    public class CTraderConnector
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;

        public CTraderConnector(string apiBaseUrl = "http://127.0.0.1:8000")
            : this(new HttpClient(), apiBaseUrl)
        {
        }

        public CTraderConnector(HttpClient httpClient, string apiBaseUrl = "http://127.0.0.1:8000")
        {
            _httpClient = httpClient;
            _apiBaseUrl = apiBaseUrl.TrimEnd('/');
        }

        public async Task<bool> SendBotUpdateAsync(
            string name,
            double profit,
            double drawdown,
            double winRate,
            int trades)
        {
            var payload = new
            {
                name = name,
                profit = profit,
                drawdown = drawdown,
                win_rate = winRate,
                trades = trades
            };

            var json = JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync($"{_apiBaseUrl}/bot/update", content);
            return response.IsSuccessStatusCode;
        }
    }

    public static class Program
    {
        public static async Task Main()
        {
            var connector = new CTraderConnector();
            var ok = await connector.SendBotUpdateAsync(
                name: "ctrader_demo_bot",
                profit: 125.0,
                drawdown: 80.0,
                winRate: 0.56,
                trades: 24
            );

            Console.WriteLine(ok ? "Bot update sent." : "Bot update failed.");
        }
    }
}
