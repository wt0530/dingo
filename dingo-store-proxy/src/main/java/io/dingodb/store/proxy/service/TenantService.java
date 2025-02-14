/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.meta.Tenant;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.TenantServiceProvider;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TenantService implements io.dingodb.meta.TenantService {

    private final InfoSchemaService infoSchemaService;
    private final TsoService tsoService = TsoService.INSTANCE;
    private final CoordinatorService coordinatorService;
    private static final TenantService INSTANCE = new TenantService();

    @AutoService(TenantServiceProvider.class)
    public static final class Provider implements TenantServiceProvider {
        @Override
        public io.dingodb.meta.TenantService get() {
            return INSTANCE;
        }
    }

    private TenantService() {
        infoSchemaService = InfoSchemaService.root();
        coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
    }

    public long tso() {
        return tsoService.cacheTso();
    }

    @Override
    public boolean createTenant(@NonNull Tenant tenant) {
        Long tenantId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TENANT)
                .count(1)
                .build()
        ).getIds().get(0);
        tenant.setId(tenantId);
        return infoSchemaService.createTenant(
            tenantId,
            tenant
        );
    }

    @Override
    public boolean updateTenant(@NonNull String oldName, String newName) {
        Tenant tenant = getTenant(oldName);
        if (tenant == null) {
            return false;
        }
        return infoSchemaService.updateTenant(
            tenant.getId(),
            Tenant.builder()
                .id(tenant.getId())
                .name(newName)
                .createdTime(tenant.getCreatedTime())
                .updatedTime(System.currentTimeMillis())
                .remarks(tenant.getRemarks())
                .isDelete(tenant.isDelete())
                .build()
        );
    }

    @Override
    public void dropTenant(@NonNull String name) {
        Tenant tenant = getTenant(name);
        if (tenant == null) {
            return;
        }
        infoSchemaService.updateTenant(
            tenant.getId(),
            Tenant.builder()
                .id(tenant.getId())
                .name(name)
                .createdTime(tenant.getCreatedTime())
                .updatedTime(System.currentTimeMillis())
                .remarks(tenant.getRemarks())
                .isDelete(true)
                .build()
        );
    }

    @Override
    public Tenant getTenant(@NonNull String name) {
        Long tenantId = getTenantId(name);
        if (tenantId == null) {
            return null;
        }
        return (Tenant) infoSchemaService.getTenant(tenantId);
    }

    @Override
    public Tenant getTenant(long id) {
        return (Tenant) infoSchemaService.getTenant(id);
    }

    @Override
    public List<Tenant> listTenant() {
        return infoSchemaService.listTenant().stream().map(t -> (Tenant) t).collect(Collectors.toList());
    }

}
