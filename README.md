# RailForge Freight Railway Operations Lab

RailForge is a dependency-free freight railway operations platform. The public surface is backed by one atomic workflow plane: domain decision, versioned state, hash-chained event stream and audit trail commit together.

The repository contains dispatch, consist, braking, yard, crew, intermodal, possession, maintenance, customs, billing and ETA domains. Feature workflows are intentionally exercised through the same control boundary so a local helper cannot masquerade as an integrated operation.
