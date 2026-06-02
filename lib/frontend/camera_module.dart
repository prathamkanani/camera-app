import 'package:camera/camera.dart';
import 'package:flutter/material.dart';
import 'package:image_picker/image_picker.dart';

enum FlashStatus { on, auto, off }

enum HdrStatus { hdr, auto, off }

enum PhotoSelection { photo, video }

enum CameraOrientation { back, front }

class CameraModule extends StatefulWidget {
  const CameraModule({super.key});

  @override
  State<CameraModule> createState() => _CameraModuleState();
}

class _CameraModuleState extends State<CameraModule> {
  late CameraController _controller;
  late Future<void> _initializeCamera;

  FlashStatus _flashStatus = .off;
  HdrStatus _hdrStatus = .hdr;
  PhotoSelection _photoSelection = .photo;
  CameraOrientation _cameraOrientation = .back;

  /// Current zoom level.
  double _currentZoomLevel = 1.0;

  /// Base zoom level before zoom started.
  double _baseZoomLevel = 1.0;

  double _minZoomLevel = 1.0;
  double _maxZoomLevel = 1.0;

  @override
  void initState() {
    super.initState();

    _initializeCamera = initializeCamera(_cameraOrientation);
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Stack(
        children: [
          Positioned.fill(
            child: FutureBuilder(
              future: _initializeCamera,
              builder: (_, snapshot) {
                if (snapshot.connectionState == .done) {
                  return GestureDetector(
                    onScaleStart: (_) {
                      _baseZoomLevel = _currentZoomLevel;
                    },

                    onScaleUpdate: (detail) async {
                      final zoom = (_baseZoomLevel * detail.scale).clamp(
                        _minZoomLevel,
                        _maxZoomLevel,
                      );

                      if (zoom != _currentZoomLevel) {
                        _currentZoomLevel = zoom;
                        await _controller.setZoomLevel(_currentZoomLevel);
                      }
                    },
                    child: CameraPreview(_controller),
                  );
                }

                return Center(child: CircularProgressIndicator());
              },
            ),
          ),

          Positioned(
            top: 0,
            left: 0,
            right: 0,
            child: Container(
              height: MediaQuery.heightOf(context) * .12,
              decoration: BoxDecoration(color: Colors.black),
              child: Row(
                mainAxisAlignment: .spaceEvenly,
                children: [
                  IconButton(
                    onPressed: () {
                      setState(() {
                        _flashStatus =
                            FlashStatus.values[(_flashStatus.index + 1) %
                                FlashStatus.values.length];
                      });
                      _controller.setFlashMode(getFlashMode(_flashStatus));
                    },
                    icon: getFlash(_flashStatus),
                  ),

                  IconButton(
                    onPressed: () {
                      setState(() {
                        _hdrStatus =
                            HdrStatus.values[(_hdrStatus.index + 1) %
                                HdrStatus.values.length];
                      });
                    },
                    icon: getHdr(_hdrStatus),
                  ),
                ],
              ),
            ),
          ),

          Positioned(
            bottom: 0,
            right: 0,
            left: 0,
            child: Container(
              height: MediaQuery.heightOf(context) * .2,
              decoration: BoxDecoration(color: Colors.black),
              padding: .all(16),
              child: Column(
                spacing: 12,
                children: [
                  Row(
                    mainAxisAlignment: .spaceEvenly,
                    children: [
                      TextButton(
                        onPressed: () {
                          if (_photoSelection == .photo) return;
                          setState(() {
                            _photoSelection = .photo;
                          });
                        },
                        child: Text(
                          'PHOTO',
                          style: TextStyle(
                            color: _photoSelection == .photo
                                ? Colors.amberAccent
                                : Colors.white,
                            fontWeight: .bold,
                          ),
                        ),
                      ),
                      TextButton(
                        onPressed: () {
                          if (_photoSelection == .video) return;
                          setState(() {
                            _photoSelection = .video;
                          });
                        },
                        child: Text(
                          'VIDEO',
                          style: TextStyle(
                            color: _photoSelection == .video
                                ? Colors.amberAccent
                                : Colors.white,
                            fontWeight: .bold,
                          ),
                        ),
                      ),
                    ],
                  ),
                  Row(
                    mainAxisAlignment: .spaceEvenly,
                    children: [
                      Container(width: 50, height: 50, color: Colors.white24),

                      GestureDetector(
                        onTap: _capturePhoto,
                        onLongPress: () {
                          _controller.startVideoRecording();
                        },
                        onLongPressEnd: (_) {
                          _captureVideo();
                        },
                        child: Stack(
                          children: [
                            Container(
                              width: 80,
                              height: 80,
                              decoration: const BoxDecoration(
                                color: Colors.white30,
                                shape: BoxShape.circle,
                              ),
                            ),
                            Positioned(
                              top: 8,
                              left: 8,
                              right: 8,
                              bottom: 8,
                              child: Container(
                                width: 70,
                                height: 70,
                                decoration: const BoxDecoration(
                                  color: Colors.white,
                                  shape: BoxShape.circle,
                                ),
                              ),
                            ),
                          ],
                        ),
                      ),

                      IconButton(
                        onPressed: () {
                          setState(() {
                            _cameraOrientation =
                                CameraOrientation
                                    .values[(_cameraOrientation.index + 1) %
                                    CameraOrientation.values.length];

                            _initializeCamera = initializeCamera(
                              _cameraOrientation,
                            );
                          });
                        },
                        icon: const Icon(
                          Icons.flip_camera_ios,
                          color: Colors.white,
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget getFlash(FlashStatus status) {
    final Color color = Colors.white;
    return switch (status) {
      .on => Icon(Icons.flash_on_rounded, color: color),
      .auto => Icon(Icons.flash_auto_rounded, color: color),
      .off => Icon(Icons.flash_off_rounded, color: color),
    };
  }

  FlashMode getFlashMode(FlashStatus status) {
    return switch (status) {
      .on => .torch,
      .auto => .auto,
      .off => .off,
    };
  }

  Widget getHdr(HdrStatus status) {
    final Color color = Colors.white;
    return switch (status) {
      .hdr => Icon(Icons.hdr_on_rounded, color: color),
      .auto => Icon(Icons.hdr_enhanced_select_rounded, color: color),
      .off => Icon(Icons.hdr_off_rounded, color: color),
    };
  }

  Future<void> initializeCamera(CameraOrientation orientation) async {
    if (_cameraOrientation != orientation) await _controller.dispose();

    final cameras = await availableCameras();

    _controller = CameraController(
      orientation == .back ? cameras.first : cameras.last,
      .high,
    );

    await _controller.initialize();

    _minZoomLevel = await _controller.getMinZoomLevel();
    _maxZoomLevel = await _controller.getMaxZoomLevel();

    _currentZoomLevel = _minZoomLevel;
  }

  Future<void> _capturePhoto() async {
    try {
      final photo = await _controller.takePicture();

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Photo stored at: ${photo.path}')),
        );
      }
    } catch (e) {
      print("Error capturing photo: $e");
    }
  }

  Future<void> _captureVideo() async {
    try {
      final video = await _controller.stopVideoRecording();

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Video stored at: ${video.path}')),
        );
      }
    } catch (e) {
      print("Error capturing photo: $e");
    }
  }

  Future<void> showGallery() async {
    
  }
}
