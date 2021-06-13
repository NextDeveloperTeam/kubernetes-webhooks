Pod Rate Limiter
----------------

Limits the number of pods with containers "starting".

The intent is to prevent "thundering herds" of starting pods,
particularly in our case the startup of intensive Spring Boot services.


TODO
----

[] Address TODOs in comments
[] Manual testing
[] Unit tests
[] Logging
[] Metrics + alerting
[] Static link openssl?