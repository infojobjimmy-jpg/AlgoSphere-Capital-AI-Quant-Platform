using cAlgo.API;
using AlgoSphere.Bots;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class FirstAlgoSphereBridge : Robot
    {
        private CTraderConnector _connector;

        protected override void OnStart()
        {
            _connector = new CTraderConnector("http://127.0.0.1:8000");
            Print("AlgoSphere bridge initialized.");
        }

        protected override async void OnStop()
        {
            bool ok = await _connector.SendBotUpdateAsync(
                name: "ctrader_first_live_bot",
                profit: 125.0,
                drawdown: 80.0,
                winRate: 0.56,
                trades: 24
            );

            Print(ok ? "POST /bot/update success" : "POST /bot/update failed");
        }
    }
}
