package sync

func (cs *ClusterSyncer) HasSynced() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if len(cs.controllers) == 0 {
		return false
	}

	for _, c := range cs.controllers {
		if !c.HasSynced() {
			return false
		}
	}
	return true
}
