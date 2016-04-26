Embulk::JavaPlugin.register_filter(
  "timestamp_format", "org.embulk.filter.TimestampFormatFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
