package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.DistributedStorage
import com.tesco.aqueduct.pipe.repository.GzipCodec
import com.tesco.aqueduct.pipe.repository.DiffingMessageRepository

class DiffingMessageStorageTest extends StorageTckSpec {
    DistributedStorage storage = new RepositoryBasedInMemoryStorage(
        1000,
        new DiffingMessageRepository(true, new GzipCodec())
    )
}
