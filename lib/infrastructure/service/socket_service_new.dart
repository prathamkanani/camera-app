import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

/// Represents the current lifecycle state of the WebSocket connection.
enum SocketConnectionState { disconnected, connecting, connected, authenticating, subscribing, subscribed, failed }

/// Configuration for retry behavior.
final class SocketRetryConfig {
  const SocketRetryConfig({
    this.maxAttempts = 5,
    this.delay = const Duration(seconds: 2),
    this.maxDelay = const Duration(seconds: 30),
    this.exponentialBackoff = true,
  }) : assert(maxAttempts > 0);

  /// Maximum number of connection attempts, including the first attempt.
  final int maxAttempts;

  /// Delay before the first retry.
  final Duration delay;

  /// Maximum delay between retries.
  final Duration maxDelay;

  /// Whether retry delays should grow exponentially.
  ///
  /// If false, every retry uses [delay].
  final bool exponentialBackoff;

  Duration delayForAttempt(int attempt) {
    if (attempt <= 0) {
      return Duration.zero;
    }

    if (!exponentialBackoff) {
      return _cap(this.delay);
    }

    final int multiplier = 1 << (attempt - 1);

    final Duration delay = Duration(milliseconds: this.delay.inMilliseconds * multiplier);

    return _cap(delay);
  }

  Duration _cap(Duration delay) {
    if (delay > maxDelay) {
      return maxDelay;
    }

    return delay;
  }
}

/// Timeout configuration for the socket lifecycle.
final class SocketTimeoutConfig {
  const SocketTimeoutConfig({
    this.socketIdTimeout = const Duration(seconds: 15),
    this.subscriptionTimeout = const Duration(seconds: 15),
  });

  /// Maximum time to wait for the socket ID.
  final Duration socketIdTimeout;

  /// Maximum time to wait for subscription confirmation.
  final Duration subscriptionTimeout;
}

/// Event names used by the Pusher/Reverb protocol.
///
/// The application-specific event is configurable through [dataEvent].
final class ReverbEventConfig {
  const ReverbEventConfig({
    this.connectionEstablished = 'pusher:connection_established',
    this.subscribe = 'pusher:subscribe',
    this.subscriptionSucceeded = 'pusher_internal:subscription_succeeded',
    this.subscriptionError = 'pusher:subscription_error',
    this.dataEvent,
    this.socketIdKey = 'socket_id',
    this.authKey = 'auth',
  });

  /// Event sent by the server after the WebSocket connection is established.
  final String connectionEstablished;

  /// Event used to subscribe to a channel.
  final String subscribe;

  /// Event sent when private-channel subscription succeeds.
  final String subscriptionSucceeded;

  /// Event sent when private-channel subscription fails.
  final String subscriptionError;

  /// Application-specific event that should be converted to [T].
  ///
  /// Example:
  /// `signing.status.updated`
  final String? dataEvent;

  /// JSON key containing the socket ID.
  final String socketIdKey;

  /// JSON key containing the authentication signature.
  final String authKey;
}

/// Configuration for how the WebSocket URL is created.
final class ReverbSocketConfig {
  const ReverbSocketConfig({
    required this.appKey,
    required this.socketUrl,
    this.protocol = '7',
    this.client = 'flutter',
    this.version = '1.0',
    this.flash = 'false',
    this.additionalQueryParameters = const <String, String>{},
  });

  final String appKey;

  /// Base WebSocket URL.
  ///
  /// Example:
  /// `ws://172.16.14.102:8888/app`
  ///
  /// or:
  /// `wss://example.com/app`
  final String socketUrl;

  final String protocol;
  final String client;
  final String version;
  final String flash;

  final Map<String, String> additionalQueryParameters;

  Uri buildUri() {
    final Uri baseUri = Uri.parse(socketUrl);

    final String path = baseUri.path.endsWith('/') ? '${baseUri.path}$appKey' : '${baseUri.path}/$appKey';

    return baseUri.replace(
      path: path,
      queryParameters: <String, String>{
        'protocol': protocol,
        'client': client,
        'version': version,
        'flash': flash,
        ...additionalQueryParameters,
      },
    );
  }
}

/// Builds the headers for the private-channel authentication request.
typedef AuthHeadersBuilder = Map<String, String> Function({required String token});

/// Builds the body for the private-channel authentication request.
typedef AuthBodyBuilder = Map<String, String> Function({required String socketId, required String channelName});

/// Extracts the authentication signature from the authentication response.
///
/// For the normal Laravel broadcasting endpoint this simply extracts
/// the `auth` property.
typedef AuthResponseParser = String Function(String responseBody);

/// Converts an application-specific socket event into [T].
///
/// [data] is the decoded event payload.
typedef SocketEventParser<T> = T Function(dynamic data);

/// Optional logger.
typedef SocketLogger = void Function(String message);

/// Generic Laravel Reverb/Pusher WebSocket service.
///
/// This class contains only WebSocket/Reverb/Pusher mechanics.
/// It has no knowledge of:
///
/// - emSigner
/// - NDA
/// - application-specific entities
/// - SharedPreferences
/// - dependency injection
/// - authentication-token storage
///
/// The consuming application provides those details through configuration
/// and method parameters.
final class ReverbSocketService<T> {
  ReverbSocketService({
    required this.socketConfig,
    required this.authUrl,
    required this.eventConfig,
    required this.eventParser,
    this.retryConfig = const SocketRetryConfig(),
    this.timeoutConfig = const SocketTimeoutConfig(),
    this.authHeadersBuilder = _defaultAuthHeadersBuilder,
    this.authBodyBuilder = _defaultAuthBodyBuilder,
    this.authResponseParser = _defaultAuthResponseParser,
    this.logger,
  });

  final ReverbSocketConfig socketConfig;

  /// Endpoint used for private-channel authentication.
  final String authUrl;

  final ReverbEventConfig eventConfig;
  final SocketRetryConfig retryConfig;
  final SocketTimeoutConfig timeoutConfig;

  final SocketEventParser<T> eventParser;

  final AuthHeadersBuilder authHeadersBuilder;
  final AuthBodyBuilder authBodyBuilder;
  final AuthResponseParser authResponseParser;

  final SocketLogger? logger;

  WebSocketChannel? _channel;

  StreamSubscription<dynamic>? _socketSubscription;

  final StreamController<T> _eventController = StreamController<T>.broadcast();

  final StreamController<SocketConnectionState> _connectionStateController =
      StreamController<SocketConnectionState>.broadcast();

  Completer<void>? _subscriptionCompleter;
  Completer<String>? _socketIdCompleter;

  String? _channelName;
  String? _token;
  String? _socketId;
  Map<String, dynamic>? _additionalSubscriptionData;

  bool _disposed = false;
  bool _disconnecting = false;
  bool _isInitializing = false;
  bool _isSubscribed = false;
  bool _reconnecting = false;

  /// Identifies the currently active socket connection.
  ///
  /// Incrementing this value invalidates callbacks belonging to an older
  /// socket. This is important when an intentional disconnect is immediately
  /// followed by a new connection, because the old socket can still deliver
  /// `onDone`/`onError` callbacks asynchronously.
  int _connectionGeneration = 0;
  int _sessionGeneration = 0;

  /// Stream of application-specific parsed socket events.
  Stream<T> get events => _eventController.stream;

  /// Stream of socket lifecycle states.
  Stream<SocketConnectionState> get connectionStates => _connectionStateController.stream;

  /// Currently active channel.
  String? get channelName => _channelName;

  /// Currently active socket ID.
  String? get socketId => _socketId;

  /// Whether the WebSocket is currently connected.
  bool get isConnected => _channel != null;

  /// Whether the service has been permanently disposed.
  bool get isDisposed => _disposed;

  /// Initializes the WebSocket connection.
  ///
  /// These parameters belong to the current connection/session and therefore
  /// are intentionally supplied here rather than in the constructor.
  ///
  /// Example:
  ///
  /// ```dart
  /// await socketService.initialize(
  ///   channelName: 'private-emsigner.$referenceId',
  ///   token: token,
  /// );
  /// ```
  Future<void> initialize({
    required String channelName,
    required String token,
    Map<String, dynamic>? additionalSubscriptionData,
  }) async {
    _ensureNotDisposed();

    if (channelName.trim().isEmpty) {
      throw const SocketServiceException('Channel name cannot be empty.');
    }

    if (token.trim().isEmpty) {
      throw const SocketServiceException('Authentication token cannot be empty.');
    }

    await disconnect();

    _channelName = channelName;
    _token = _normalizeToken(token);
    _additionalSubscriptionData = additionalSubscriptionData == null
        ? null
        : Map<String, dynamic>.from(additionalSubscriptionData);
    ++_sessionGeneration;
    _isInitializing = true;
    _isSubscribed = false;

    try {
      SocketServiceException? lastNetworkException;

      for (int attempt = 1; attempt <= retryConfig.maxAttempts; attempt++) {
        _ensureNotDisposed();

        try {
          _log(
            'Socket initialization attempt '
            '$attempt/${retryConfig.maxAttempts}',
          );

          await _initializeSocket(additionalSubscriptionData: _additionalSubscriptionData);

          _log(
            'Socket initialized successfully '
            'on attempt $attempt.',
          );

          return;
        } catch (error, stackTrace) {
          final SocketServiceException exception = _toSocketException(error);

          _log(
            'Socket initialization attempt $attempt failed: '
            '$exception\n$stackTrace',
          );

          await _cleanupSocketAfterFailure();

          if (!_isRetryableNetworkError(error)) {
            _emitState(SocketConnectionState.failed);
            rethrow;
          }

          lastNetworkException = exception;

          if (attempt >= retryConfig.maxAttempts) {
            break;
          }

          final Duration retryDelay = retryConfig.delayForAttempt(attempt);

          _log(
            'Network failure detected. '
            'Retrying in ${retryDelay.inSeconds} seconds...',
          );

          _emitState(SocketConnectionState.connecting);

          await Future<void>.delayed(retryDelay);
        }
      }

      _emitState(SocketConnectionState.failed);

      throw SocketServiceException(
        'Unable to connect to the socket after '
        '${retryConfig.maxAttempts} attempts.',
        lastNetworkException,
      );
    } finally {
      _isInitializing = false;
    }
  }

  /// Performs one complete connection attempt.
  Future<void> _initializeSocket({Map<String, dynamic>? additionalSubscriptionData}) async {
    _subscriptionCompleter = Completer<void>();
    _socketIdCompleter = Completer<String>();
    _socketId = null;

    _emitState(SocketConnectionState.connecting);

    final Uri socketUri = socketConfig.buildUri();

    _log('Connecting to socket: $socketUri');

    final int generation = ++_connectionGeneration;
    final WebSocketChannel channel = WebSocketChannel.connect(socketUri);

    _channel = channel;

    _socketSubscription = channel.stream.listen(
      (dynamic message) => _handleSocketMessage(message, generation),
      onError: (Object error, StackTrace stackTrace) => _handleSocketError(error, stackTrace, generation),
      onDone: () => _handleSocketDone(generation),
      cancelOnError: false,
    );

    try {
      await channel.ready;
    } catch (error) {
      throw _toSocketException(error);
    }

    _emitState(SocketConnectionState.connected);

    _log('Connected to socket.');

    await _waitForSocketId();

    await _authenticateAndSubscribe(additionalSubscriptionData: additionalSubscriptionData);

    await waitUntilSubscribed();

    _log('Successfully subscribed to $_channelName');
  }

  /// Waits for the server to provide the socket ID.
  Future<String> _waitForSocketId() async {
    final Completer<String>? completer = _socketIdCompleter;

    if (completer == null) {
      throw const SocketServiceException('Socket ID completer is not initialized.');
    }

    if (completer.isCompleted) {
      return completer.future;
    }

    try {
      return await completer.future.timeout(timeoutConfig.socketIdTimeout);
    } on TimeoutException {
      throw const SocketServiceException('Timed out waiting for socket ID.');
    }
  }

  /// Authenticates and subscribes to the configured private channel.
  Future<void> _authenticateAndSubscribe({Map<String, dynamic>? additionalSubscriptionData}) async {
    final String? socketId = _socketId;
    final String? channelName = _channelName;
    final String? token = _token;

    if (socketId == null || socketId.isEmpty) {
      throw const SocketServiceException('Socket ID is not available.');
    }

    if (channelName == null || channelName.isEmpty) {
      throw const SocketServiceException('Channel name is not available.');
    }

    if (token == null || token.isEmpty) {
      throw const SocketServiceException('Authentication token is not available.');
    }

    _emitState(SocketConnectionState.authenticating);

    _log('Authenticating private channel: $channelName');

    final Map<String, String> headers = authHeadersBuilder(token: token);

    final Map<String, String> body = authBodyBuilder(socketId: socketId, channelName: channelName);

    late final http.Response response;

    try {
      response = await http.post(Uri.parse(authUrl), headers: headers, body: body);
    } catch (error) {
      throw _toSocketException(error);
    }

    _log(
      'Private channel auth response: '
      '${response.statusCode}',
    );

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw SocketServiceException(
        'Private-channel authentication failed '
        'with status ${response.statusCode}: ${response.body}',
      );
    }

    final String auth = authResponseParser(response.body);

    if (auth.isEmpty) {
      throw const SocketServiceException(
        'Private-channel authentication response '
        'does not contain a valid authentication value.',
      );
    }

    _emitState(SocketConnectionState.subscribing);

    final Map<String, dynamic> subscribeData = <String, dynamic>{
      eventConfig.authKey: auth,
      'channel': channelName,
      ...?additionalSubscriptionData,
    };

    final Map<String, dynamic> subscribeMessage = <String, dynamic>{
      'event': eventConfig.subscribe,
      'data': subscribeData,
    };

    _log('Subscribing to private channel: $channelName');

    final WebSocketChannel? channel = _channel;

    if (channel == null) {
      throw const SocketServiceException('Cannot subscribe because the socket is not connected.');
    }

    try {
      channel.sink.add(jsonEncode(subscribeMessage));
    } catch (error) {
      throw _toSocketException(error);
    }
  }

  /// Waits until the server confirms channel subscription.
  ///
  /// [timeout] can be overridden for a particular operation without changing
  /// the service-wide configuration.
  Future<void> waitUntilSubscribed({Duration? timeout}) async {
    _ensureNotDisposed();

    final Completer<void>? completer = _subscriptionCompleter;

    if (completer == null) {
      throw const SocketServiceException('Subscription has not been initialized.');
    }

    if (completer.isCompleted) {
      return;
    }

    try {
      await completer.future.timeout(timeout ?? timeoutConfig.subscriptionTimeout);
    } on TimeoutException {
      throw const SocketServiceException('Timed out waiting for channel subscription.');
    }
  }

  /// Handles all incoming Pusher/Reverb messages.
  void _handleSocketMessage(dynamic rawMessage, int generation) {
    if (generation != _connectionGeneration || _disposed) {
      return;
    }
    try {
      _log('Socket message: $rawMessage');

      if (rawMessage is! String) {
        return;
      }

      final dynamic decoded = jsonDecode(rawMessage);

      if (decoded is! Map) {
        return;
      }

      final Map<String, dynamic> message = Map<String, dynamic>.from(decoded);

      final String? eventName = message['event']?.toString();

      if (eventName == null || eventName.isEmpty) {
        return;
      }

      _log('Received socket event: $eventName');

      switch (eventName) {
        case final event when event == eventConfig.subscriptionSucceeded:
          _handleSubscriptionSucceeded();
          return;

        case final event when event == eventConfig.subscriptionError:
          _handleSubscriptionError(message);
          return;

        case final event when event == eventConfig.connectionEstablished:
          _handleConnectionEstablished(message);
          return;

        case final event when event == eventConfig.dataEvent:
          _handleDataEvent(message['data']);
          return;

        default:
          _log(
            'Unhandled socket event: $eventName\n'
            'Data: ${message['data']}',
          );
      }
    } catch (error, stackTrace) {
      _log(
        'Failed to process socket message: '
        '$error\n$stackTrace',
      );
    }
  }

  void _handleSubscriptionSucceeded() {
    _isSubscribed = true;

    _log(
      'Private channel subscription succeeded: '
      '$_channelName',
    );

    _emitState(SocketConnectionState.subscribed);

    final Completer<void>? completer = _subscriptionCompleter;

    if (completer != null && !completer.isCompleted) {
      completer.complete();
    }
  }

  void _handleSubscriptionError(Map<String, dynamic> message) {
    final dynamic data = message['data'];

    _log('Private channel subscription failed: $data');

    final SocketServiceException error = SocketServiceException('Failed to subscribe to $_channelName: $data');

    _emitState(SocketConnectionState.failed);

    _completeSubscriptionWithError(error);
  }

  /// Converts the application-specific event payload to [T].
  void _handleDataEvent(dynamic rawData) {
    try {
      dynamic data = rawData;

      /// Pusher/Reverb commonly sends event data as a JSON string.
      if (data is String) {
        data = jsonDecode(data);
      }

      final T result = eventParser(data);

      if (!_eventController.isClosed) {
        _eventController.add(result);
      }
    } catch (error, stackTrace) {
      _log(
        'Failed to parse socket event '
        '${eventConfig.dataEvent}: '
        '$error\n$stackTrace',
      );
    }
  }

  /// Extracts the socket ID from the connection-established event.
  void _handleConnectionEstablished(Map<String, dynamic> event) {
    try {
      dynamic rawData = event['data'];

      if (rawData is String) {
        rawData = jsonDecode(rawData);
      }

      if (rawData is! Map) {
        throw const SocketServiceException('Connection-established data is invalid.');
      }

      final Map<String, dynamic> data = Map<String, dynamic>.from(rawData);

      final String? socketId = data[eventConfig.socketIdKey]?.toString();

      if (socketId == null || socketId.isEmpty) {
        final SocketServiceException error = const SocketServiceException(
          'Connection-established event did not '
          'contain a socket ID.',
        );

        if (!(_socketIdCompleter?.isCompleted ?? true)) {
          _socketIdCompleter!.completeError(error);
        }

        return;
      }

      _log('Received socket ID: $socketId');

      _socketId = socketId;

      if (!(_socketIdCompleter?.isCompleted ?? true)) {
        _socketIdCompleter!.complete(socketId);
      }
    } catch (error, stackTrace) {
      _log(
        'Failed to extract socket ID: '
        '$error\n$stackTrace',
      );

      if (!(_socketIdCompleter?.isCompleted ?? true)) {
        _socketIdCompleter!.completeError(error, stackTrace);
      }
    }
  }

  void _handleSocketError(Object error, StackTrace stackTrace, int generation) {
    if (generation != _connectionGeneration || _disposed) {
      return;
    }

    _log(
      'Socket error: '
      '$error\n$stackTrace',
    );

    _emitState(SocketConnectionState.failed);

    _completeSubscriptionWithError(error);

    if (_isSubscribed && !_isInitializing && !_disconnecting) {
      _isSubscribed = false;
      unawaited(_reconnectAfterUnexpectedDisconnect(_sessionGeneration));
    }
  }

  void _handleSocketDone(int generation) {
    if (generation != _connectionGeneration || _disposed) {
      return;
    }

    _log('Socket disconnected.');

    _emitState(SocketConnectionState.disconnected);

    _completeSubscriptionWithError(const SocketServiceException('Socket disconnected unexpectedly.'));

    if (_isSubscribed && !_isInitializing && !_disconnecting) {
      _isSubscribed = false;
      unawaited(_reconnectAfterUnexpectedDisconnect(_sessionGeneration));
    }
  }

  /// Reconnects an already-established session after an unexpected socket
  /// failure. This is deliberately separate from [initialize] because the
  /// current channel and token must be retained for the reconnect.
  Future<void> _reconnectAfterUnexpectedDisconnect(int sessionGeneration) async {
    if (_reconnecting || _disposed || _disconnecting) {
      return;
    }

    if (sessionGeneration != _sessionGeneration) {
      return;
    }

    final String? channelName = _channelName;
    final String? token = _token;

    if (channelName == null || token == null) {
      _log('Cannot reconnect because the active socket session is unavailable.');
      return;
    }

    _reconnecting = true;

    try {
      SocketServiceException? lastNetworkException;

      for (int attempt = 1; attempt <= retryConfig.maxAttempts; attempt++) {
        if (_disposed || _disconnecting || sessionGeneration != _sessionGeneration) {
          return;
        }

        _log(
          'Automatic socket reconnection attempt '
          '$attempt/${retryConfig.maxAttempts}',
        );

        try {
          await _cleanupSocketAfterFailure();
          _isInitializing = true;

          await _initializeSocket(additionalSubscriptionData: _additionalSubscriptionData);

          _isInitializing = false;
          _log('Automatic socket reconnection succeeded on attempt $attempt.');
          return;
        } catch (error, stackTrace) {
          _isInitializing = false;

          final SocketServiceException exception = _toSocketException(error);

          _log(
            'Automatic socket reconnection attempt $attempt failed: '
            '$exception\n$stackTrace',
          );

          await _cleanupSocketAfterFailure();

          if (!_isRetryableNetworkError(error)) {
            _emitState(SocketConnectionState.failed);
            return;
          }

          lastNetworkException = exception;

          if (attempt >= retryConfig.maxAttempts) {
            break;
          }

          final Duration retryDelay = retryConfig.delayForAttempt(attempt);

          _log(
            'Automatic reconnection will retry in '
            '${retryDelay.inSeconds} seconds...',
          );

          _emitState(SocketConnectionState.connecting);
          await Future<void>.delayed(retryDelay);
        }
      }

      if (!_disposed && !_disconnecting && sessionGeneration == _sessionGeneration) {
        _emitState(SocketConnectionState.failed);
        _log(
          'Automatic socket reconnection exhausted after '
          '${retryConfig.maxAttempts} attempts. '
          'Last error: $lastNetworkException',
        );
      }
    } finally {
      _reconnecting = false;
      _isInitializing = false;
    }
  }

  /// Disconnects the current socket.
  ///
  /// The service itself remains reusable.
  Future<void> disconnect() async {
    if (_disconnecting) {
      return;
    }

    _disconnecting = true;
    _isSubscribed = false;
    _isInitializing = false;

    // Invalidate callbacks from the socket being intentionally closed and
    // invalidate any automatic-reconnect operation belonging to that session.
    ++_connectionGeneration;
    ++_sessionGeneration;

    try {
      if (_channel == null && _socketSubscription == null) {
        _resetSocketState();
        _emitState(SocketConnectionState.disconnected);
        return;
      }
      _log('Disconnecting socket...');

      final Completer<void>? subscriptionCompleter = _subscriptionCompleter;

      if (subscriptionCompleter != null && !subscriptionCompleter.isCompleted) {
        subscriptionCompleter.completeError(const SocketServiceException('Socket disconnected.'));
      }

      try {
        await _socketSubscription?.cancel();
      } catch (error) {
        _log('Failed to cancel socket subscription: $error');
      }

      _socketSubscription = null;

      try {
        await _channel?.sink.close();
      } catch (error) {
        _log('Failed to close socket: $error');
      }

      _channel = null;

      _resetSocketState();

      _emitState(SocketConnectionState.disconnected);

      _log('Socket disconnected.');
    } finally {
      _disconnecting = false;
    }
  }

  /// Permanently disposes the service.
  ///
  /// After this method:
  ///
  /// - The socket is disconnected.
  /// - Streams are closed.
  /// - [initialize] can no longer be called.
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }

    _log('Disposing socket service...');

    await disconnect();

    _disposed = true;

    await _eventController.close();
    await _connectionStateController.close();

    _resetSocketState();

    _log('Socket service disposed.');
  }

  bool _isRetryableNetworkError(Object error) {
    if (error is SocketException) {
      return true;
    }

    if (error is TimeoutException) {
      return true;
    }

    if (error is http.ClientException) {
      return true;
    }

    if (error is WebSocketChannelException) {
      return true;
    }

    if (error is SocketServiceException) {
      final Object? cause = error.error;

      if (cause == null) {
        return false;
      }

      return _isRetryableNetworkError(cause);
    }

    return false;
  }

  SocketServiceException _toSocketException(Object error) {
    if (error is SocketServiceException) {
      return error;
    }

    if (_isRetryableNetworkError(error)) {
      return SocketServiceException('Network connection failed.', error);
    }

    return SocketServiceException('Failed to communicate with the socket.', error);
  }

  Future<void> _cleanupSocketAfterFailure() async {
    try {
      await _socketSubscription?.cancel();
    } catch (error) {
      _log(
        'Failed to cancel failed socket subscription: '
        '$error',
      );
    }

    _socketSubscription = null;

    try {
      await _channel?.sink.close();
    } catch (error) {
      _log('Failed to close failed socket: $error');
    }

    _channel = null;
    _isSubscribed = false;

    _socketId = null;
    _socketIdCompleter = null;
    _subscriptionCompleter = null;
  }

  void _resetSocketState() {
    _channel = null;
    _socketSubscription = null;
    _isSubscribed = false;
    _socketId = null;
    _socketIdCompleter = null;
    _subscriptionCompleter = null;
    _channelName = null;
    _token = null;
    _additionalSubscriptionData = null;
  }

  String _normalizeToken(String token) {
    return token.replaceFirst(RegExp(r'^Bearer\s+', caseSensitive: false), '');
  }

  void _emitState(SocketConnectionState state) {
    if (_disposed || _connectionStateController.isClosed) {
      return;
    }

    _connectionStateController.add(state);
  }

  void _completeSubscriptionWithError(Object error) {
    final Completer<void>? completer = _subscriptionCompleter;

    if (completer != null && !completer.isCompleted) {
      completer.completeError(error);
    }
  }

  void _ensureNotDisposed() {
    if (_disposed) {
      throw const SocketServiceException('ReverbSocketService has already been disposed.');
    }
  }

  void _log(String message) {
    logger?.call(message);
  }

  static Map<String, String> _defaultAuthHeadersBuilder({required String token}) {
    return <String, String>{
      'Accept': 'application/json',
      'Authorization': 'Bearer $token',
      'Content-Type': 'application/x-www-form-urlencoded',
    };
  }

  static Map<String, String> _defaultAuthBodyBuilder({required String socketId, required String channelName}) {
    return <String, String>{'socket_id': socketId, 'channel_name': channelName};
  }

  static String _defaultAuthResponseParser(String responseBody) {
    final dynamic decoded = jsonDecode(responseBody);

    if (decoded is! Map) {
      throw const SocketServiceException('Invalid authentication response.');
    }

    final dynamic auth = decoded['auth'];

    if (auth == null) {
      throw const SocketServiceException('Authentication response does not contain "auth".');
    }

    return auth.toString();
  }
}

/// Generic exception for WebSocket/Reverb operations.
final class SocketServiceException implements Exception {
  const SocketServiceException(this.message, [this.error]);

  final String message;
  final Object? error;

  @override
  String toString() {
    if (error == null) {
      return message;
    }

    return '$message Cause: $error';
  }
}
