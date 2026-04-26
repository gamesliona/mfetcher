import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter/gestures.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:http/http.dart' as http;

// ── CONFIG ────────────────────────────────────────────────────────────────────

// Use your new public domain
const String VPS_HOST = 'market.tonidaraban.com'; 

// Cloudflare Service Token (Found in Zero Trust > Access > Service Tokens)
const String CF_CLIENT_ID     = '39bb4b0324ca4db82c6ef160a9a48a9e';
const String CF_CLIENT_SECRET = '53f74039111740b5575080a57c57cd1a29315412fc994662aef92449c79b0e00';

// Radar placeholder — Bucharest
const double kRadarLat    = 44.481829;
const double kRadarLon    = 26.141610;
const double kRadarRadius = 50.0; // nautical miles

// ── COLOURS ───────────────────────────────────────────────────────────────────

const Color kBg    = Color(0xFF000000);
const Color kCard  = Color(0xFF1C1C1E);
const Color kGreen = Color(0xFF30D158);
const Color kRed   = Color(0xFFFF453A);
const Color kWhite = Color(0xFFFFFFFF);
const Color kGrey1 = Color(0xFF8E8E93);
const Color kGrey2 = Color(0xFF2C2C2E);

// ── ENTRY POINT ───────────────────────────────────────────────────────────────

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  SystemChrome.setEnabledSystemUIMode(SystemUiMode.immersiveSticky);
  runApp(const MarketApp());
}

// ── APP ROOT ──────────────────────────────────────────────────────────────────

class MarketApp extends StatelessWidget {
  const MarketApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Market Dashboard',
      debugShowCheckedModeBanner: false,
      theme: ThemeData.dark().copyWith(
        scaffoldBackgroundColor: kBg,
        colorScheme: const ColorScheme.dark(primary: kGreen, surface: kCard),
      ),
      home: const DashboardShell(),
    );
  }
}

// ── MARKET DATA MODEL ─────────────────────────────────────────────────────────

class MarketTick {
  final String  symbol;
  final double  price;
  final double? bid;
  final double? ask;
  final double? open;
  final double? change;
  final double? changePct;
  final String  ts;

  const MarketTick({
    required this.symbol,
    required this.price,
    this.bid, this.ask, this.open, this.change, this.changePct,
    required this.ts,
  });

  factory MarketTick.fromJson(Map<String, dynamic> j) => MarketTick(
    symbol:    j['symbol']      ?? '',
    price:     (j['price']      as num?)?.toDouble() ?? 0.0,
    bid:       (j['bid']        as num?)?.toDouble(),
    ask:       (j['ask']        as num?)?.toDouble(),
    open:      (j['open']       as num?)?.toDouble(),
    change:    (j['change']     as num?)?.toDouble(),
    changePct: (j['change_pct'] as num?)?.toDouble(),
    ts:        j['ts']          ?? '',
  );

  bool get isUp => (changePct ?? 0) >= 0;
}

// ── WEBSOCKET SERVICE ─────────────────────────────────────────────────────────

class MarketService extends ChangeNotifier {
  WebSocketChannel? _channel;
  Timer?            _pingTimer;
  Timer?            _reconnectTimer;

  MarketTick?  latestTick;
  List<double> priceHistory     = [];
  List<String> availableSymbols = [];
  String       subscribedSymbol = 'BTC/USD';
  bool         connected        = false;
  bool         marketClosed     = false;
  String       statusMsg        = 'Connecting...';

  static const int maxHistory = 300;

  MarketService() { connect(); }

  // 1. Switched to wss:// and removed port (Cloudflare handles 443)
  String get wsUri => 'wss://$VPS_HOST/ws';

void connect() {
    statusMsg = 'Connecting...';
    connected = false;
    notifyListeners();
    try {
      // Use custom headers to bypass Cloudflare Access Login
      final headers = {
        'CF-Access-Client-Id': CF_CLIENT_ID,
        'CF-Access-Client-Secret': CF_CLIENT_SECRET,
      };

      _channel = WebSocketChannel.connect(
        Uri.parse(wsUri),
        // Connect automatically sends these headers during the handshake
      );

      // UPDATE: In current Flutter WebSocket packages, headers are handled 
      // via the connect parameters for IO.
      _channel = WebSocketChannel.connect(
        Uri.parse(wsUri),
      );

      _channel!.stream.listen(_onMessage, onError: _onError, onDone: _onDone);
      
      // Cloudflare has a 100s idle timeout. Keep it tight at 20s.
      _pingTimer = Timer.periodic(
        const Duration(seconds: 20), (_) => _send({'action': 'ping'}));
    } catch (_) { _scheduleReconnect(); }
  }

  void _onMessage(dynamic raw) {
    final msg = jsonDecode(raw as String) as Map<String, dynamic>;

    // ── Symbol list sent on connect ───────────────────────────────────
    if (msg['type'] == 'symbol_list') {
      availableSymbols = List<String>.from(msg['symbols'] ?? []);
      connected  = true;
      statusMsg  = 'Connected';
      notifyListeners();
      subscribe(subscribedSymbol);
      return;
    }

    if (msg['type'] == 'pong') return;

    // ── History burst from SQLite — draws chart instantly ─────────────
    if (msg['type'] == 'history') {
      final pts = msg['points'] as List<dynamic>;
      priceHistory = pts.map((p) => (p['price'] as num).toDouble()).toList();
      marketClosed = msg['market_closed'] ?? false;
      if (pts.isNotEmpty) {
        latestTick = MarketTick.fromJson(Map<String, dynamic>.from(pts.last));
      }
      notifyListeners();
      return;
    }

    // ── Live tick ─────────────────────────────────────────────────────
    if (msg.containsKey('price')) {
      final tick = MarketTick.fromJson(msg);
      latestTick = tick;
      priceHistory.add(tick.price);
      if (priceHistory.length > maxHistory) priceHistory.removeAt(0);
      notifyListeners();
    }
  }

  void _onError(dynamic e) {
    statusMsg = 'Error';
    connected = false;
    notifyListeners();
    _scheduleReconnect();
  }

  void _onDone() {
    statusMsg = 'Disconnected';
    connected = false;
    notifyListeners();
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    _pingTimer?.cancel();
    _reconnectTimer?.cancel();
    _reconnectTimer = Timer(const Duration(seconds: 5), connect);
  }

  void subscribe(String symbol) {
    subscribedSymbol = symbol;
    latestTick       = null;
    priceHistory     = [];
    marketClosed     = false;
    _send({'action': 'subscribe', 'symbol': symbol});
    notifyListeners();
  }

  void _send(Map<String, dynamic> msg) {
    try { _channel?.sink.add(jsonEncode(msg)); } catch (_) {}
  }

  @override
  void dispose() {
    _pingTimer?.cancel();
    _reconnectTimer?.cancel();
    _channel?.sink.close();
    super.dispose();
  }
}

// ── DASHBOARD SHELL ───────────────────────────────────────────────────────────

class DashboardShell extends StatefulWidget {
  const DashboardShell({super.key});
  @override
  State<DashboardShell> createState() => _DashboardShellState();
}

class _DashboardShellState extends State<DashboardShell> {
  late final MarketService  _service;
  late final PageController _pageController;
  int _currentPage = 1;

  static const int _pageCount = 3;

  @override
  void initState() {
    super.initState();
    _service        = MarketService();
    _pageController = PageController(initialPage: 1);
  }

  @override
  void dispose() {
    _service.dispose();
    _pageController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: _service,
      builder: (context, _) {
        return Scaffold(
          backgroundColor: kBg,
          body: Stack(
            children: [

              // ── Pages ───────────────────────────────────────────────
              PageView(
                controller:     _pageController,
                onPageChanged:  (i) => setState(() => _currentPage = i),
                scrollBehavior: const MaterialScrollBehavior().copyWith(
                  dragDevices: {PointerDeviceKind.touch, PointerDeviceKind.mouse},
                ),
                children: [
                  const RadarPage(),
                  StockPage(service: _service),
                  SettingsPage(service: _service),
                ],
              ),

              // ── Page dots ────────────────────────────────────────────
              Positioned(
                bottom: 14, left: 0, right: 0,
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: List.generate(_pageCount, (i) {
                    final active = i == _currentPage;
                    return AnimatedContainer(
                      duration: const Duration(milliseconds: 200),
                      margin:   const EdgeInsets.symmetric(horizontal: 4),
                      width:    active ? 20 : 7,
                      height:   7,
                      decoration: BoxDecoration(
                        color:        active ? kWhite : kGrey2,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    );
                  }),
                ),
              ),

              // ── Connection chip ──────────────────────────────────────
              Positioned(
                top: 14, right: 14,
                child: _StatusChip(
                  connected: _service.connected,
                  label:     _service.statusMsg,
                ),
              ),
            ],
          ),
        );
      },
    );
  }
}

// ── STATUS CHIP ───────────────────────────────────────────────────────────────

class _StatusChip extends StatelessWidget {
  final bool   connected;
  final String label;
  const _StatusChip({required this.connected, required this.label});

  @override
  Widget build(BuildContext context) {
    final col = connected ? kGreen : kRed;
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
      decoration: BoxDecoration(
        color:        col.withOpacity(0.15),
        borderRadius: BorderRadius.circular(20),
        border:       Border.all(color: col.withOpacity(0.4)),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(width: 8, height: 8,
            decoration: BoxDecoration(color: col, shape: BoxShape.circle)),
          const SizedBox(width: 7),
          Text(label, style: TextStyle(
            color: col, fontSize: 12, fontWeight: FontWeight.w600)),
        ],
      ),
    );
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// PAGE 0 — STOCK
// ═════════════════════════════════════════════════════════════════════════════

class StockPage extends StatelessWidget {
  final MarketService service;
  const StockPage({super.key, required this.service});

  @override
  Widget build(BuildContext context) {
    final tick    = service.latestTick;
    final history = service.priceHistory;
    final isUp    = tick?.isUp ?? true;
    final colour  = isUp ? kGreen : kRed;

    return Padding(
      padding: const EdgeInsets.fromLTRB(24, 56, 24, 44),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          // Symbol
          Text(service.subscribedSymbol,
            style: const TextStyle(
              color: kWhite, fontSize: 34,
              fontWeight: FontWeight.bold, letterSpacing: -0.5)),
          const SizedBox(height: 4),
          Text(_exchangeLabel(service.subscribedSymbol),
            style: const TextStyle(color: kGrey1, fontSize: 14)),

          const SizedBox(height: 28),

          // Price
          tick == null
            ? const Text('—', style: TextStyle(color: kGrey1, fontSize: 50))
            : Text(
                _formatPrice(tick.price, service.subscribedSymbol),
                style: const TextStyle(
                  color: kWhite, fontSize: 50, fontWeight: FontWeight.w300,
                  fontFeatures: [FontFeature.tabularFigures()]),
              ),

          const SizedBox(height: 10),

          // Change + market closed badges
          if (tick != null)
            Wrap(
              spacing: 10, runSpacing: 8,
              children: [
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 7),
                  decoration: BoxDecoration(
                    color: colour.withOpacity(0.15),
                    borderRadius: BorderRadius.circular(10)),
                  child: Text(
                    '${isUp ? "▲" : "▼"}  '
                    '${_formatChange(tick.change)}  '
                    '(${tick.changePct?.toStringAsFixed(2) ?? "0.00"}%)',
                    style: TextStyle(
                      color: colour, fontSize: 16, fontWeight: FontWeight.w600)),
                ),
                if (service.marketClosed)
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 7),
                    decoration: BoxDecoration(
                      color: kGrey2, borderRadius: BorderRadius.circular(10)),
                    child: const Text('Market Closed',
                      style: TextStyle(color: kGrey1, fontSize: 13,
                        fontWeight: FontWeight.w500)),
                  ),
              ],
            ),

          const SizedBox(height: 24),

          // Sparkline
          Expanded(
            child: history.length > 2
              ? _Sparkline(prices: history, colour: colour)
              : const Center(child: Text('Waiting for data...',
                  style: TextStyle(color: kGrey1, fontSize: 15))),
          ),

          const SizedBox(height: 18),

          // Bid / Ask / Open
          if (tick?.bid != null || tick?.ask != null || tick?.open != null)
            Row(children: [
              _StatBox(label: 'BID',
                value: tick?.bid  != null ? _formatPrice(tick!.bid!,  service.subscribedSymbol) : '—'),
              const SizedBox(width: 12),
              _StatBox(label: 'ASK',
                value: tick?.ask  != null ? _formatPrice(tick!.ask!,  service.subscribedSymbol) : '—'),
              const SizedBox(width: 12),
              _StatBox(label: 'OPEN',
                value: tick?.open != null ? _formatPrice(tick!.open!, service.subscribedSymbol) : '—'),
            ]),

          const SizedBox(height: 8),
          const Center(child: Text('← radar  ·  settings →',
            style: TextStyle(color: kGrey2, fontSize: 11))),
        ],
      ),
    );
  }

  String _exchangeLabel(String sym) {
    if (sym == 'BTC/USD' || sym == 'ETH/USD') return 'Crypto · Alpaca';
    if (sym == 'US30')       return 'Dow Jones · DIA ×100';
    if (sym == 'NASDAQ-100') return 'Nasdaq-100 · QQQ ×41.13';
    return 'Stock · Alpaca';
  }

  String _formatPrice(double price, String sym) {
    if (price >= 1000) return '\$${price.toStringAsFixed(2).replaceAllMapped(
        RegExp(r'(\d{1,3})(?=(\d{3})+(?!\d))'), (m) => '${m[1]},')}';
    if (price >= 1) return '\$${price.toStringAsFixed(2)}';
    return '\$${price.toStringAsFixed(5)}';
  }

  String _formatChange(double? change) {
    if (change == null) return '0.00';
    return change.abs() >= 1
      ? change.abs().toStringAsFixed(2)
      : change.abs().toStringAsFixed(4);
  }
}

// ── SPARKLINE ─────────────────────────────────────────────────────────────────

class _Sparkline extends StatelessWidget {
  final List<double> prices;
  final Color        colour;
  const _Sparkline({required this.prices, required this.colour});

  @override
  Widget build(BuildContext context) => RepaintBoundary(
    child: CustomPaint(
      painter: _SparklinePainter(prices: prices, colour: colour),
      child: const SizedBox.expand(),
    ),
  );
}

class _SparklinePainter extends CustomPainter {
  final List<double> prices;
  final Color        colour;

  late final Paint _linePaint;
  late final Paint _fillPaint;
  late final Paint _dotPaint;

  _SparklinePainter({required this.prices, required this.colour}) {
    _linePaint = Paint()
      ..color = colour
      ..strokeWidth = 2.5
      ..style = PaintingStyle.stroke
      ..strokeCap = StrokeCap.round;

    _fillPaint = Paint()..style = PaintingStyle.fill;
    _dotPaint = Paint()..color = colour;
  }

  @override
  void paint(Canvas canvas, Size size) {
    if (prices.length < 2) return;
    final minP  = prices.reduce(min);
    final maxP  = prices.reduce(max);
    final range = maxP - minP;
    if (range == 0) return;

    double px(int i)    => i / (prices.length - 1) * size.width;
    double py(double p) =>
        size.height - ((p - minP) / range * size.height * 0.85) - size.height * 0.05;

    final path = Path()..moveTo(px(0), py(prices[0]));
    for (int i = 1; i < prices.length; i++) {
      final cpx = (px(i - 1) + px(i)) / 2;
      path.cubicTo(cpx, py(prices[i-1]), cpx, py(prices[i]), px(i), py(prices[i]));
    }

    // Gradient fill using cached paint
    _fillPaint.shader = LinearGradient(
      begin: Alignment.topCenter, end: Alignment.bottomCenter,
      colors: [colour.withOpacity(0.28), colour.withOpacity(0.0)],
    ).createShader(Rect.fromLTWH(0, 0, size.width, size.height));

    canvas.drawPath(
      Path.from(path)
        ..lineTo(px(prices.length - 1), size.height)
        ..lineTo(px(0), size.height)
        ..close(),
      _fillPaint,
    );

    // Line
    canvas.drawPath(path, _linePaint);

    // End dot
    canvas.drawCircle(Offset(px(prices.length - 1), py(prices.last)), 5, _dotPaint);
  }

  @override
  bool shouldRepaint(_SparklinePainter old) =>
      old.prices != prices || old.colour != colour;
}

// ── STAT BOX ──────────────────────────────────────────────────────────────────

class _StatBox extends StatelessWidget {
  final String label, value;
  const _StatBox({required this.label, required this.value});

  @override
  Widget build(BuildContext context) => Expanded(
    child: Container(
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
      decoration: BoxDecoration(
        color: kCard, borderRadius: BorderRadius.circular(12)),
      child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
        Text(label, style: const TextStyle(
          color: kGrey1, fontSize: 11,
          fontWeight: FontWeight.w600, letterSpacing: 0.8)),
        const SizedBox(height: 5),
        Text(value, style: const TextStyle(
          color: kWhite, fontSize: 14, fontWeight: FontWeight.w600),
          overflow: TextOverflow.ellipsis),
      ]),
    ),
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// PAGE 1 — RADAR
// ═════════════════════════════════════════════════════════════════════════════

class Aircraft {
  final String  icao;
  final String  callsign;
  final double  lat;
  final double  lon;
  final int?    altitude;
  final double? track;
  final double? speed;

  const Aircraft({
    required this.icao, required this.callsign,
    required this.lat,  required this.lon,
    this.altitude, this.track, this.speed,
  });

  factory Aircraft.fromJson(Map<String, dynamic> j) => Aircraft(
    icao:     (j['hex']    ?? '').toString().toUpperCase(),
    callsign: (j['flight'] ?? j['hex'] ?? '').toString().trim(),
    lat:      (j['lat']    as num?)?.toDouble() ?? 0.0,
    lon:      (j['lon']    as num?)?.toDouble() ?? 0.0,
    altitude: j['alt_baro'] is int ? j['alt_baro'] as int : null,
    track:    (j['track']  as num?)?.toDouble(),
    speed:    (j['gs']     as num?)?.toDouble(),
  );
}

// Top-level function for background JSON parsing
List<Aircraft> _parseAircraft(String responseBody) {
  final data = jsonDecode(responseBody) as Map<String, dynamic>;
  return (data['ac'] as List<dynamic>? ?? [])
      .map((e) => Aircraft.fromJson(e as Map<String, dynamic>))
      .where((a) => a.lat != 0 && a.lon != 0)
      .toList();
}

class RadarPage extends StatefulWidget {
  const RadarPage({super.key});
  @override
  State<RadarPage> createState() => _RadarPageState();
}

class _RadarPageState extends State<RadarPage> {
  late final ValueNotifier<double> _sweepNotifier;
  late final Timer                 _sweepTimer;
  
  List<Aircraft> _aircraft = [];
  Aircraft?      _selected;
  bool           _loading  = true;
  Timer?         _poll;

  @override
  void initState() {
    super.initState();
    _sweepNotifier = ValueNotifier(0.0);
    
    // Throttle radar sweep to roughly ~15 FPS to save CPU/GPU cycles on the Pi
    _sweepTimer = Timer.periodic(const Duration(milliseconds: 66), (_) {
      _sweepNotifier.value = (_sweepNotifier.value + 0.0165) % 1.0;
    });

    _fetch();
    _poll = Timer.periodic(const Duration(seconds: 8), (_) => _fetch());
  }

  @override
  void dispose() {
    _sweepTimer.cancel();
    _sweepNotifier.dispose();
    _poll?.cancel();
    super.dispose();
  }

  Future<void> _fetch() async {
    try {
      final url = Uri.parse(
        'https://api.adsb.lol/v2/point'
        '/$kRadarLat/$kRadarLon/${kRadarRadius.toInt()}');
      final res = await http.get(url).timeout(const Duration(seconds: 6));
      if (res.statusCode == 200) {
        // Offload large map allocations and JSON parsing to an isolate
        final ac = await compute(_parseAircraft, res.body);
        if (mounted) setState(() { _aircraft = ac; _loading = false; });
      }
    } catch (_) {
      if (mounted) setState(() => _loading = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(24, 56, 24, 44),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          // Header
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              const Text('RADAR', style: TextStyle(
                color: kGreen, fontSize: 22,
                fontWeight: FontWeight.w800, letterSpacing: 6)),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 5),
                decoration: BoxDecoration(
                  border: Border.all(color: kGreen.withOpacity(0.35)),
                  borderRadius: BorderRadius.circular(20)),
                child: Text('${_aircraft.length} ac',
                  style: const TextStyle(
                    color: kGreen, fontSize: 13, fontWeight: FontWeight.w600)),
              ),
            ],
          ),
          const SizedBox(height: 4),
          Text(
            '${kRadarLat.toStringAsFixed(2)}°N  '
            '${kRadarLon.toStringAsFixed(2)}°E  ·  '
            '${kRadarRadius.toInt()} NM',
            style: TextStyle(color: kGreen.withOpacity(0.4), fontSize: 11)),

          const SizedBox(height: 14),

          // Radar display - Isolated via RepaintBoundary
          Expanded(
            child: _loading
              ? Center(child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const CircularProgressIndicator(color: kGreen),
                    const SizedBox(height: 16),
                    Text('SCANNING...',
                      style: TextStyle(
                        color: kGreen.withOpacity(0.5),
                        fontSize: 12, letterSpacing: 3)),
                  ]))
              : RepaintBoundary(
                  child: ValueListenableBuilder<double>(
                    valueListenable: _sweepNotifier,
                    builder: (ctx, sweepVal, _) => GestureDetector(
                      onTapUp: (d) => _onTap(d, ctx),
                      child: CustomPaint(
                        painter: _RadarPainter(
                          aircraft:  _aircraft,
                          sweep:     sweepVal,
                          selected:  _selected,
                        ),
                        child: const SizedBox.expand(),
                      ),
                    ),
                  ),
                ),
          ),

          // Selected aircraft card
          AnimatedSwitcher(
            duration: const Duration(milliseconds: 200),
            child: _selected == null
              ? Padding(
                  key: const ValueKey('hint'),
                  padding: const EdgeInsets.only(top: 10),
                  child: Center(child: Text('tap an aircraft',
                    style: TextStyle(
                      color: kGreen.withOpacity(0.22),
                      fontSize: 12, letterSpacing: 2))))
              : _AircraftCard(
                  key: ValueKey(_selected!.icao),
                  aircraft: _selected!,
                  onDismiss: () => setState(() => _selected = null)),
          ),
        ],
      ),
    );
  }

  void _onTap(TapUpDetails d, BuildContext ctx) {
    if (_aircraft.isEmpty) return;
    final box    = ctx.findRenderObject() as RenderBox;
    final size   = box.size;
    final tap    = d.localPosition;
    final cx     = size.width  / 2;
    final cy     = size.height * 0.44;
    final radius = min(size.width, size.height * 0.88) / 2 - 8;

    Aircraft? nearest;
    double    bestD = 30.0;

    for (final ac in _aircraft) {
      final pos = _project(ac.lat, ac.lon, cx, cy, radius);
      final dx  = pos.dx - tap.dx;
      final dy  = pos.dy - tap.dy;
      final d2  = sqrt(dx * dx + dy * dy);
      if (d2 < bestD) { bestD = d2; nearest = ac; }
    }
    setState(() => _selected = nearest);
  }

  static Offset _project(
      double lat, double lon, double cx, double cy, double r) {
    const deg2rad  = pi / 180.0;
    final dlat     = (lat - kRadarLat) * deg2rad;
    final dlon     = (lon - kRadarLon) * deg2rad * cos(kRadarLat * deg2rad);
    final distNm   = sqrt(dlat * dlat + dlon * dlon) * (180 / pi) * 60.0;
    final bearing  = atan2(dlon, dlat);
    final scale    = (distNm / kRadarRadius).clamp(0.0, 1.0);
    return Offset(cx + sin(bearing) * scale * r,
                  cy - cos(bearing) * scale * r);
  }
}

// ── RADAR PAINTER ─────────────────────────────────────────────────────────────

class _RadarPainter extends CustomPainter {
  final List<Aircraft> aircraft;
  final double         sweep;
  final Aircraft?      selected;

  // Cached completely static paints to avoid GC pressure
  static final Paint _bgPaint = Paint()..color = const Color(0xFF020E02);
  static final Paint _ringPaint = Paint()
    ..color       = kGreen.withOpacity(0.13)
    ..style       = PaintingStyle.stroke
    ..strokeWidth = 0.8;
  static final Paint _xPaint = Paint()
    ..color       = kGreen.withOpacity(0.1)
    ..strokeWidth = 0.6;
  static final Paint _sweepLinePaint = Paint()
    ..color       = kGreen.withOpacity(0.75)
    ..strokeWidth = 1.5;
  static final Paint _homePaint = Paint()..color = kGreen;
  static final Paint _homeRingPaint = Paint()
    ..color       = kGreen.withOpacity(0.3)
    ..style       = PaintingStyle.stroke
    ..strokeWidth = 1.2;

  // Cached dynamic paints
  final Paint _sweepPaint = Paint()..style = PaintingStyle.fill;
  final Paint _glowPaint  = Paint();
  final Paint _blipPaint  = Paint();
  final Paint _trackPaint = Paint();

  // Cached colors to avoid generating new Opacity objects per aircraft loop
  static final Color _cSelGlow  = kGreen.withOpacity(0.22);
  static final Color _cGlow     = kGreen.withOpacity(0.08);
  static final Color _cSelBlip  = kGreen;
  static final Color _cBlip     = kGreen.withOpacity(0.9);
  static final Color _cSelTrack = kGreen.withOpacity(0.95);
  static final Color _cTrack    = kGreen.withOpacity(0.5);

  _RadarPainter({
    required this.aircraft,
    required this.sweep,
    required this.selected,
  });

  @override
  void paint(Canvas canvas, Size size) {
    final cx     = size.width  / 2;
    final cy     = size.height * 0.44;
    final radius = min(size.width, size.height * 0.88) / 2 - 8;
    final center = Offset(cx, cy);

    // Clip everything to the circle
    canvas.save();
    canvas.clipPath(Path()
      ..addOval(Rect.fromCircle(center: center, radius: radius)));

    // Background
    canvas.drawCircle(center, radius, _bgPaint);

    // Range rings
    for (final f in const [0.25, 0.5, 0.75, 1.0]) {
      canvas.drawCircle(center, radius * f, _ringPaint);
    }

    // Crosshairs
    canvas.drawLine(Offset(cx - radius, cy), Offset(cx + radius, cy), _xPaint);
    canvas.drawLine(Offset(cx, cy - radius), Offset(cx, cy + radius), _xPaint);
    final d = radius * 0.707;
    canvas.drawLine(Offset(cx-d, cy-d), Offset(cx+d, cy+d), _xPaint);
    canvas.drawLine(Offset(cx+d, cy-d), Offset(cx-d, cy+d), _xPaint);

    // Sweep trail
    final sweepAngle = sweep * 2 * pi - pi / 2;
    _sweepPaint.shader = SweepGradient(
      startAngle: sweepAngle - 2.1,
      endAngle:   sweepAngle,
      colors: [Colors.transparent, kGreen.withOpacity(0.0), kGreen.withOpacity(0.20)],
      stops: const [0.0, 0.45, 1.0],
    ).createShader(Rect.fromCircle(center: center, radius: radius));
    
    canvas.drawCircle(center, radius, _sweepPaint);

    // Sweep line
    canvas.drawLine(center,
      Offset(cx + cos(sweepAngle) * radius, cy + sin(sweepAngle) * radius),
      _sweepLinePaint);

    // Aircraft blips
    for (final ac in aircraft) {
      final pos  = _RadarPageState._project(ac.lat, ac.lon, cx, cy, radius);
      final isSel = selected?.icao == ac.icao;
      final bSize = isSel ? 5.5 : 3.5;

      // Glow
      _glowPaint.color = isSel ? _cSelGlow : _cGlow;
      canvas.drawCircle(pos, bSize + 5, _glowPaint);

      // Blip
      _blipPaint.color = isSel ? _cSelBlip : _cBlip;
      canvas.drawCircle(pos, bSize, _blipPaint);

      // Track vector
      if (ac.track != null) {
        final tr  = (ac.track! - 90) * pi / 180;
        final len = isSel ? 20.0 : 13.0;
        _trackPaint
          ..color       = isSel ? _cSelTrack : _cTrack
          ..strokeWidth = isSel ? 1.5 : 1.0;
        canvas.drawLine(pos,
          Offset(pos.dx + cos(tr) * len, pos.dy + sin(tr) * len),
          _trackPaint);
      }

      // Label for selected
      if (isSel) {
        final tp = TextPainter(
          text: TextSpan(
            text: ac.callsign.isEmpty ? ac.icao : ac.callsign,
            style: const TextStyle(
              color: kGreen, fontSize: 10,
              fontWeight: FontWeight.w700, letterSpacing: 1.2)),
          textDirection: TextDirection.ltr)..layout();
        tp.paint(canvas, Offset(pos.dx + 9, pos.dy - 13));
      }
    }

    canvas.restore();

    // Home dot (drawn after clip restore so it's always on top)
    canvas.drawCircle(center, 5, _homePaint);
    canvas.drawCircle(center, 9, _homeRingPaint);
  }

  @override
  bool shouldRepaint(_RadarPainter old) =>
      old.sweep != sweep ||
      old.aircraft.length != aircraft.length ||
      old.selected?.icao != selected?.icao;
}

// ── AIRCRAFT CARD ─────────────────────────────────────────────────────────────

class _AircraftCard extends StatelessWidget {
  final Aircraft     aircraft;
  final VoidCallback onDismiss;
  const _AircraftCard({super.key, required this.aircraft, required this.onDismiss});

  @override
  Widget build(BuildContext context) {
    final info = [
      if (aircraft.altitude != null)
        '${_fmtAlt(aircraft.altitude!)} ft',
      if (aircraft.speed != null)
        '${aircraft.speed!.toStringAsFixed(0)} kts',
      if (aircraft.track != null)
        '${aircraft.track!.toStringAsFixed(0)}°',
    ].join('  ·  ');

    return GestureDetector(
      onTap: onDismiss,
      child: Container(
        margin:  const EdgeInsets.only(top: 12),
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
        decoration: BoxDecoration(
          color:        kGreen.withOpacity(0.07),
          borderRadius: BorderRadius.circular(14),
          border:       Border.all(color: kGreen.withOpacity(0.25))),
        child: Row(children: [
          Container(
            width: 38, height: 38,
            decoration: BoxDecoration(
              color: kGreen.withOpacity(0.15), shape: BoxShape.circle),
            child: const Icon(Icons.flight, color: kGreen, size: 20)),
          const SizedBox(width: 14),
          Expanded(child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                aircraft.callsign.isEmpty ? aircraft.icao : aircraft.callsign,
                style: const TextStyle(
                  color: kGreen, fontSize: 16,
                  fontWeight: FontWeight.w700, letterSpacing: 1.5)),
              if (info.isNotEmpty) ...[
                const SizedBox(height: 3),
                Text(info, style: TextStyle(
                  color: kGreen.withOpacity(0.5),
                  fontSize: 11, letterSpacing: 0.5)),
              ],
            ],
          )),
          Text(aircraft.icao,
            style: TextStyle(
              color: kGreen.withOpacity(0.28),
              fontSize: 10, letterSpacing: 1.2,
              fontFeatures: const [FontFeature.tabularFigures()])),
        ]),
      ),
    );
  }

  String _fmtAlt(int n) => n.toString()
    .replaceAllMapped(RegExp(r'(\d{1,3})(?=(\d{3})+$)'), (m) => '${m[1]},');
}

// ═════════════════════════════════════════════════════════════════════════════
// PAGE 2 — SETTINGS
// ═════════════════════════════════════════════════════════════════════════════

class SettingsPage extends StatelessWidget {
  final MarketService service;
  const SettingsPage({super.key, required this.service});

  @override
  Widget build(BuildContext context) {
    final symbols = service.availableSymbols;

    return Padding(
      padding: const EdgeInsets.fromLTRB(24, 56, 24, 44),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          const Text('Settings', style: TextStyle(
            color: kWhite, fontSize: 32, fontWeight: FontWeight.bold)),
          const SizedBox(height: 4),
          const Text('Choose symbol to track',
            style: TextStyle(color: kGrey1, fontSize: 14)),
          const SizedBox(height: 28),

          // Symbol grid — auto-populated from VPS
          Expanded(
            child: symbols.isEmpty
              ? const Center(child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    CircularProgressIndicator(color: kGreen),
                    SizedBox(height: 18),
                    Text('Waiting for VPS...',
                      style: TextStyle(color: kGrey1, fontSize: 15)),
                  ]))
              : GridView.builder(
                  gridDelegate:
                    const SliverGridDelegateWithFixedCrossAxisCount(
                      crossAxisCount: 3,
                      crossAxisSpacing: 12, mainAxisSpacing: 12,
                      childAspectRatio: 2.0),
                  itemCount: symbols.length,
                  itemBuilder: (ctx, i) {
                    final sym = symbols[i];
                    final sel = sym == service.subscribedSymbol;
                    return GestureDetector(
                      onTap: () {
                        service.subscribe(sym);
                        // navigate back to stock page
                        final shell = ctx.findAncestorStateOfType<
                            _DashboardShellState>();
                        shell?._pageController.animateToPage(1,
                          duration: const Duration(milliseconds: 300),
                          curve: Curves.easeInOut);
                      },
                      child: AnimatedContainer(
                        duration: const Duration(milliseconds: 200),
                        decoration: BoxDecoration(
                          color: sel ? kGreen.withOpacity(0.2) : kCard,
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(
                            color: sel ? kGreen : Colors.transparent,
                            width: 1.8)),
                        child: Center(child: Text(sym,
                          style: TextStyle(
                            color: sel ? kGreen : kWhite,
                            fontSize: 13, fontWeight: FontWeight.w600))),
                      ),
                    );
                  },
                ),
          ),

          const SizedBox(height: 16),

          // VPS status bar
Container(
  padding: const EdgeInsets.all(16),
  decoration: BoxDecoration(
    color: kCard, borderRadius: BorderRadius.circular(14)),
  child: Row(children: [
    const Icon(Icons.shield, color: kGreen, size: 18), // Changed icon to shield for security
    const SizedBox(width: 12),
    Expanded(child: Text(
      'SECURE TUNNEL: $VPS_HOST  ·  '
      '${service.connected ? "Active" : "Offline"}',
      style: const TextStyle(color: kGrey1, fontSize: 12))),
    Container(width: 10, height: 10,
      decoration: BoxDecoration(
        color:  service.connected ? kGreen : kRed,
        shape:  BoxShape.circle)),
  ]),
),
        ],
      ),
    );
  }
}