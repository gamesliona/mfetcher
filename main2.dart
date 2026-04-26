import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:flutter/gestures.dart';

// ── CONFIG ────────────────────────────────────────────────────────────────────

const String VPS_HOST = '192.168.122.63'; // ← Change to your VPS IP
const int    VPS_PORT = 8000;

// ── COLOURS (Apple dark mode) ─────────────────────────────────────────────────

const Color kBg      = Color(0xFF000000);
const Color kCard    = Color(0xFF1C1C1E);
const Color kGreen   = Color(0xFF30D158);
const Color kRed     = Color(0xFFFF453A);
const Color kWhite   = Color(0xFFFFFFFF);
const Color kGrey1   = Color(0xFF8E8E93);
const Color kGrey2   = Color(0xFF2C2C2E);
const Color kGrey3   = Color(0xFF3A3A3C);

// ── ENTRY POINT ───────────────────────────────────────────────────────────────

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // Hide system UI for kiosk mode on Pi
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
        colorScheme: const ColorScheme.dark(
          primary: kGreen,
          surface: kCard,
        ),
      ),
      home: const DashboardShell(),
    );
  }
}

// ── MARKET DATA MODEL ─────────────────────────────────────────────────────────

class MarketTick {
  final String symbol;
  final double price;
  final double? bid;
  final double? ask;
  final double? open;
  final double? change;
  final double? changePct;
  final String ts;

  const MarketTick({
    required this.symbol,
    required this.price,
    this.bid,
    this.ask,
    this.open,
    this.change,
    this.changePct,
    required this.ts,
  });

  factory MarketTick.fromJson(Map<String, dynamic> j) => MarketTick(
        symbol:    j['symbol'] ?? '',
        price:     (j['price']  as num?)?.toDouble() ?? 0.0,
        bid:       (j['bid']    as num?)?.toDouble(),
        ask:       (j['ask']    as num?)?.toDouble(),
        open:      (j['open']   as num?)?.toDouble(),
        change:    (j['change'] as num?)?.toDouble(),
        changePct: (j['change_pct'] as num?)?.toDouble(),
        ts:        j['ts'] ?? '',
      );

  bool get isUp => (changePct ?? 0) >= 0;
}

// ── WEBSOCKET SERVICE ─────────────────────────────────────────────────────────

class MarketService extends ChangeNotifier {
  WebSocketChannel? _channel;
  Timer?            _pingTimer;
  Timer?            _reconnectTimer;

  MarketTick?   latestTick;
  List<double>  priceHistory = [];
  List<String>  availableSymbols = [];
  String        subscribedSymbol = 'BTC/USD';
  bool          connected = false;
  String        statusMsg = 'Connecting...';
  bool          marketClosed = false; 

  static const int maxHistory = 300;

  MarketService() {
    connect();
  }

  String get wsUri => 'ws://$VPS_HOST:$VPS_PORT/ws';

  void connect() {
    statusMsg = 'Connecting...';
    connected = false;
    notifyListeners();

    try {
      _channel = WebSocketChannel.connect(Uri.parse(wsUri));
      _channel!.stream.listen(
        _onMessage,
        onError: _onError,
        onDone:  _onDone,
      );
      // Start ping to keep connection alive
      _pingTimer = Timer.periodic(
        const Duration(seconds: 20),
        (_) => _send({'action': 'ping'}),
      );
    } catch (e) {
      _scheduleReconnect();
    }
  }

  void _onMessage(dynamic raw) {
    final Map<String, dynamic> msg = jsonDecode(raw as String);

    // Symbol list on first connect
if (msg['type'] == 'symbol_list') {
  availableSymbols = List<String>.from(msg['symbols'] ?? []);
  connected  = true;
  statusMsg  = 'Connected';
  notifyListeners();
  subscribe(subscribedSymbol);
  return;
}

if (msg['type'] == 'pong') return;

// History burst — pre-load chart instantly on connect
    if (msg['type'] == 'history') {
      final points = msg['points'] as List<dynamic>;
      priceHistory = points
        .map((p) => (p['price'] as num).toDouble())
        .toList();
      marketClosed = msg['market_closed'] ?? false;   // ← add this
      if (points.isNotEmpty) {
        latestTick = MarketTick.fromJson(
          Map<String, dynamic>.from(points.last)
        );
      }
      notifyListeners();
      return;
    }
  notifyListeners();
  return;
}

    // Price tick
    if (msg.containsKey('price')) {
      final tick = MarketTick.fromJson(msg);
      latestTick = tick;
      priceHistory.add(tick.price);
      if (priceHistory.length > maxHistory) {
        priceHistory.removeAt(0);
      }
      notifyListeners();
    }
  }

  void _onError(dynamic e) {
    statusMsg = 'Connection error';
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
    try {
      _channel?.sink.add(jsonEncode(msg));
    } catch (_) {}
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
// Swipeable PageView — add new pages here as you build more apps

class DashboardShell extends StatefulWidget {
  const DashboardShell({super.key});

  @override
  State<DashboardShell> createState() => _DashboardShellState();
}

class _DashboardShellState extends State<DashboardShell> {
  late final MarketService _service;
  late final PageController _pageController;
  int _currentPage = 0;

  // Page definitions — add Weather, Flights etc here later
  static const List<String> _pageTitles = [
    'Market',
    'Settings',
    // 'Weather',   // ← uncomment when ready
    // 'Flights',
  ];

  @override
  void initState() {
    super.initState();
    _service        = MarketService();
    _pageController = PageController();
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

              // ── Swipeable pages ──────────────────────────────────────────
              PageView(
               controller: _pageController,
               onPageChanged: (i) => setState(() => _currentPage = i),
               scrollBehavior: const MaterialScrollBehavior().copyWith(
                 dragDevices: {
                    PointerDeviceKind.touch,
                    PointerDeviceKind.mouse,
                    },
                  ),
                  children: [
                  StockPage(service: _service),
                  SettingsPage(service: _service),
                  // WeatherPage(),   // ← future
                  // FlightsPage(),   // ← future
                ],
              ),

              // ── Page indicator dots (bottom centre) ──────────────────────
              Positioned(
                bottom: 16,
                left: 0,
                right: 0,
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: List.generate(_pageTitles.length, (i) {
                    final active = i == _currentPage;
                    return AnimatedContainer(
                      duration: const Duration(milliseconds: 200),
                      margin: const EdgeInsets.symmetric(horizontal: 3),
                      width:  active ? 18 : 6,
                      height: 6,
                      decoration: BoxDecoration(
                        color:        active ? kWhite : kGrey2,
                        borderRadius: BorderRadius.circular(3),
                      ),
                    );
                  }),
                ),
              ),

              // ── Connection status chip (top right) ───────────────────────
              Positioned(
                top: 12, right: 12,
                child: _StatusChip(connected: _service.connected,
                                   label:     _service.statusMsg),
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
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
      decoration: BoxDecoration(
        color:        connected ? kGreen.withOpacity(0.15)
                                : kRed.withOpacity(0.15),
        borderRadius: BorderRadius.circular(20),
        border:       Border.all(
          color: connected ? kGreen.withOpacity(0.4)
                           : kRed.withOpacity(0.4),
        ),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 6, height: 6,
            decoration: BoxDecoration(
              color:  connected ? kGreen : kRed,
              shape:  BoxShape.circle,
            ),
          ),
          const SizedBox(width: 6),
          Text(label,
            style: TextStyle(
              color:    connected ? kGreen : kRed,
              fontSize: 10,
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}

// ── STOCK PAGE ────────────────────────────────────────────────────────────────

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
      padding: const EdgeInsets.fromLTRB(20, 52, 20, 40),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [

          // ── Symbol + exchange ──────────────────────────────────────────
          Text(service.subscribedSymbol,
            style: const TextStyle(
              color:      kWhite,
              fontSize:   28,
              fontWeight: FontWeight.bold,
              letterSpacing: -0.5,
            ),
          ),
          const SizedBox(height: 2),
          Text(
            _exchangeLabel(service.subscribedSymbol),
            style: const TextStyle(color: kGrey1, fontSize: 12),
          ),

          const SizedBox(height: 24),

          // ── Price ──────────────────────────────────────────────────────
          tick == null
            ? const Text('—', style: TextStyle(color: kGrey1, fontSize: 42))
            : Text(
                _formatPrice(tick.price, service.subscribedSymbol),
                style: const TextStyle(
                  color:      kWhite,
                  fontSize:   42,
                  fontWeight: FontWeight.w300,
                  fontFeatures: [FontFeature.tabularFigures()],
                ),
              ),

          const SizedBox(height: 8),

          // ── Change badge ───────────────────────────────────────────────
          if (tick != null) ...[
            Row(
              children: [
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
                  decoration: BoxDecoration(
                    color:        colour.withOpacity(0.15),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Text(
                    '${isUp ? '▲' : '▼'}  '
                    '${_formatChange(tick.change, service.subscribedSymbol)}  '
                    '(${tick.changePct?.toStringAsFixed(2) ?? '0.00'}%)',
                    style: TextStyle(
                      color:      colour,
                      fontSize:   13,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                if (service.marketClosed) ...[
                  const SizedBox(width: 8),
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 5),
                    decoration: BoxDecoration(
                      color:        kGrey2,
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: const Text(
                      'Market Closed',
                      style: TextStyle(
                        color:      kGrey1,
                        fontSize:   11,
                        fontWeight: FontWeight.w500,
                      ),
                    ),
                  ),
                ],
              ],
            ),
          ],

          const SizedBox(height: 28),

          // ── Sparkline chart ────────────────────────────────────────────
          Expanded(
            child: history.length > 2
              ? _Sparkline(prices: history, colour: colour)
              : Center(
                  child: Text(
                    'Waiting for data...',
                    style: TextStyle(color: kGrey1, fontSize: 13),
                  ),
                ),
          ),

          const SizedBox(height: 20),

          // ── Bid / Ask row ──────────────────────────────────────────────
          if (tick?.bid != null || tick?.ask != null)
            Row(
              children: [
                _StatBox(label: 'BID',
                  value: tick?.bid != null
                    ? _formatPrice(tick!.bid!, service.subscribedSymbol)
                    : '—'),
                const SizedBox(width: 12),
                _StatBox(label: 'ASK',
                  value: tick?.ask != null
                    ? _formatPrice(tick!.ask!, service.subscribedSymbol)
                    : '—'),
                const SizedBox(width: 12),
                _StatBox(label: 'OPEN',
                  value: tick?.open != null
                    ? _formatPrice(tick!.open!, service.subscribedSymbol)
                    : '—'),
              ],
            ),

          const SizedBox(height: 8),

          // ── Swipe hint ─────────────────────────────────────────────────
          Center(
            child: Text('swipe for settings',
              style: TextStyle(color: kGrey2, fontSize: 10)),
          ),
        ],
      ),
    );
  }

  String _exchangeLabel(String sym) {
    if (sym.contains('/')) return sym.contains('USD') && sym.startsWith('BTC') || sym.startsWith('ETH') ? 'Crypto · Alpaca' : 'Forex · Twelve Data';
    if (['US30', 'SP500'].contains(sym)) return 'Index · Twelve Data';
    if (sym == 'XAU/USD') return 'Commodity · Twelve Data';
    return 'NASDAQ · Twelve Data';
  }

  String _formatPrice(double price, String sym) {
    if (price >= 1000) return '\$${price.toStringAsFixed(2).replaceAllMapped(RegExp(r'(\d{1,3})(?=(\d{3})+(?!\d))'), (m) => '${m[1]},')}';
    if (price >= 1)    return '\$${price.toStringAsFixed(2)}';
    return '\$${price.toStringAsFixed(5)}';
  }

  String _formatChange(double? change, String sym) {
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
  Widget build(BuildContext context) {
    return CustomPaint(
      painter: _SparklinePainter(prices: prices, colour: colour),
      child: const SizedBox.expand(),
    );
  }
}

class _SparklinePainter extends CustomPainter {
  final List<double> prices;
  final Color        colour;
  _SparklinePainter({required this.prices, required this.colour});

  @override
  void paint(Canvas canvas, Size size) {
    if (prices.length < 2) return;

    final minP = prices.reduce((a, b) => a < b ? a : b);
    final maxP = prices.reduce((a, b) => a > b ? a : b);
    final range = (maxP - minP).abs();
    if (range == 0) return;

    double x(int i)   => i / (prices.length - 1) * size.width;
    double y(double p) => size.height - ((p - minP) / range * size.height * 0.85) - size.height * 0.05;

    // Build path
    final path = Path()..moveTo(x(0), y(prices[0]));
    for (int i = 1; i < prices.length; i++) {
      // Smooth curve
      final cpX = (x(i - 1) + x(i)) / 2;
      path.cubicTo(cpX, y(prices[i - 1]), cpX, y(prices[i]), x(i), y(prices[i]));
    }

    // Gradient fill
    final fillPath = Path.from(path)
      ..lineTo(x(prices.length - 1), size.height)
      ..lineTo(x(0), size.height)
      ..close();

    canvas.drawPath(
      fillPath,
      Paint()
        ..shader = LinearGradient(
            begin: Alignment.topCenter,
            end:   Alignment.bottomCenter,
            colors: [colour.withOpacity(0.25), colour.withOpacity(0.0)],
          ).createShader(Rect.fromLTWH(0, 0, size.width, size.height))
        ..style = PaintingStyle.fill,
    );

    // Line
    canvas.drawPath(
      path,
      Paint()
        ..color       = colour
        ..strokeWidth = 2.0
        ..style       = PaintingStyle.stroke
        ..strokeCap   = StrokeCap.round,
    );

    // Dot at latest price
    canvas.drawCircle(
      Offset(x(prices.length - 1), y(prices.last)),
      4,
      Paint()..color = colour,
    );
  }

  @override
  bool shouldRepaint(_SparklinePainter old) => old.prices != prices;
}

// ── STAT BOX ──────────────────────────────────────────────────────────────────

class _StatBox extends StatelessWidget {
  final String label;
  final String value;
  const _StatBox({required this.label, required this.value});

  @override
  Widget build(BuildContext context) {
    return Expanded(
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
        decoration: BoxDecoration(
          color:        kCard,
          borderRadius: BorderRadius.circular(10),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(label,
              style: const TextStyle(color: kGrey1, fontSize: 9,
                fontWeight: FontWeight.w600, letterSpacing: 0.8)),
            const SizedBox(height: 4),
            Text(value,
              style: const TextStyle(color: kWhite, fontSize: 12,
                fontWeight: FontWeight.w600),
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ),
      ),
    );
  }
}

// ── SETTINGS PAGE ─────────────────────────────────────────────────────────────

class SettingsPage extends StatelessWidget {
  final MarketService service;
  const SettingsPage({super.key, required this.service});

  @override
  Widget build(BuildContext context) {
    final symbols = service.availableSymbols;

    return Padding(
      padding: const EdgeInsets.fromLTRB(20, 52, 20, 40),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text('Settings',
            style: TextStyle(color: kWhite, fontSize: 28,
              fontWeight: FontWeight.bold)),
          const SizedBox(height: 4),
          const Text('Choose symbol to track',
            style: TextStyle(color: kGrey1, fontSize: 13)),
          const SizedBox(height: 24),

          // Symbol grid
Expanded(
  child: symbols.isEmpty
    ? const Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            CircularProgressIndicator(color: kGreen),
            SizedBox(height: 16),
            Text('Waiting for VPS...',
              style: TextStyle(color: kGrey1, fontSize: 13)),
          ],
        ),
      )
    : GridView.builder(
        gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
          crossAxisCount:   3,
          crossAxisSpacing: 10,
          mainAxisSpacing:  10,
          childAspectRatio: 2.2,
        ),
        itemCount: symbols.length,
        itemBuilder: (context, i) {
          final sym      = symbols[i];
          final selected = sym == service.subscribedSymbol;
          return GestureDetector(
            onTap: () {
              service.subscribe(sym);
              Navigator.of(context).maybePop();
            },
            child: AnimatedContainer(
              duration: const Duration(milliseconds: 200),
              decoration: BoxDecoration(
                color:        selected ? kGreen.withOpacity(0.2) : kCard,
                borderRadius: BorderRadius.circular(10),
                border:       Border.all(
                  color: selected ? kGreen : Colors.transparent,
                  width: 1.5,
                ),
              ),
              child: Center(
                child: Text(sym,
                  style: TextStyle(
                    color:      selected ? kGreen : kWhite,
                    fontSize:   12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
            ),
          );
        },
      ),
),

          // VPS info
          Container(
            padding: const EdgeInsets.all(14),
            decoration: BoxDecoration(
              color:        kCard,
              borderRadius: BorderRadius.circular(12),
            ),
            child: Row(
              children: [
                const Icon(Icons.cloud, color: kGrey1, size: 16),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    'VPS: $VPS_HOST:$VPS_PORT  ·  '
                    '${service.connected ? "Connected" : "Disconnected"}',
                    style: const TextStyle(color: kGrey1, fontSize: 11),
                  ),
                ),
                Container(
                  width: 8, height: 8,
                  decoration: BoxDecoration(
                    color:  service.connected ? kGreen : kRed,
                    shape:  BoxShape.circle,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
