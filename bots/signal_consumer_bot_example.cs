using System;
using System.Threading.Tasks;
using cAlgo.API;
using AlgoSphere.Bots;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class AlgoSphereSignalConsumerBot : Robot
    {
        [Parameter("Algo Sphere Bot Name", DefaultValue = "ctrader_first_live_bot")]
        public string AlgoSphereBotName { get; set; }

        [Parameter("API URL", DefaultValue = "http://127.0.0.1:8000")]
        public string ApiUrl { get; set; }

        [Parameter("Poll Interval (sec)", DefaultValue = 15, MinValue = 5)]
        public int PollIntervalSec { get; set; }

        [Parameter("Initial Volume", DefaultValue = 10000, MinValue = 1000)]
        public double InitialVolume { get; set; }

        [Parameter("Volume Step", DefaultValue = 1000, MinValue = 100)]
        public double VolumeStep { get; set; }

        [Parameter("Volume Floor", DefaultValue = 1000, MinValue = 100)]
        public double VolumeFloor { get; set; }

        [Parameter("Volume Cap", DefaultValue = 50000, MinValue = 1000)]
        public double VolumeCap { get; set; }

        private ControlSignalClient _signalClient;
        private bool _entriesEnabled = true;
        private double _currentVolume;
        private string _lastKnownAction = "NO_CHANGE";
        private string _lastKnownState = "MONITOR";
        private bool _pollInProgress = false;

        protected override void OnStart()
        {
            _signalClient = new ControlSignalClient(ApiUrl);
            _currentVolume = InitialVolume;
            Timer.Start(PollIntervalSec);
            Print("Signal consumer started for bot '{0}'. Poll every {1}s.", AlgoSphereBotName, PollIntervalSec);
        }

        protected override async void OnTimer()
        {
            if (_pollInProgress)
            {
                return;
            }
            _pollInProgress = true;
            try
            {
                await PollAndApplyAsync();
            }
            catch (Exception ex)
            {
                Print("Signal poll error: {0}", ex.Message);
            }
            finally
            {
                _pollInProgress = false;
            }
        }

        private async Task PollAndApplyAsync()
        {
            var signal = await _signalClient.GetSignalForBotAsync(AlgoSphereBotName);
            if (signal == null)
            {
                Print("No signal for '{0}'. Keep last settings. action={1}, volume={2}, entriesEnabled={3}",
                    AlgoSphereBotName, _lastKnownAction, _currentVolume, _entriesEnabled);
                return;
            }

            _lastKnownState = signal.control_state;
            _lastKnownAction = signal.recommended_action;

            switch (signal.recommended_action)
            {
                case "STOP":
                    _entriesEnabled = false;
                    break;
                case "LOWER_VOLUME":
                    _entriesEnabled = true;
                    _currentVolume = Math.Max(VolumeFloor, _currentVolume - VolumeStep);
                    break;
                case "INCREASE_VOLUME":
                    _entriesEnabled = true;
                    _currentVolume = Math.Min(VolumeCap, _currentVolume + VolumeStep);
                    break;
                default:
                    // NO_CHANGE and unknown actions: keep current local settings.
                    break;
            }

            Print(
                "Signal: state={0}, action={1}, active={2}, effective_capital={3}; local_policy => entriesEnabled={4}, volume={5}",
                signal.control_state,
                signal.recommended_action,
                signal.control_active,
                signal.effective_capital,
                _entriesEnabled,
                _currentVolume
            );
        }

        protected override void OnStop()
        {
            Print("Signal consumer stopped. last_state={0}, last_action={1}, entriesEnabled={2}, volume={3}",
                _lastKnownState, _lastKnownAction, _entriesEnabled, _currentVolume);
        }
    }
}
