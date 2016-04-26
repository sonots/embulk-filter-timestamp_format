Embulk::JavaPlugin.register_filter(
  "timestamp_format", "org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
