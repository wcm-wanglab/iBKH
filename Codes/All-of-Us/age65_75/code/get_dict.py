dictt={'age65_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/f4b76758-3274-4bda-be95-1dd92c07d274/vcfs',
      'age65_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/f27e309a-6692-4069-954c-0f65c133a01c/vcfs',
      'age66_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/32003b9a-70e5-4e66-b282-64eb4f61238a/vcfs',
      'age66_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/29efe80c-61fa-4de7-9692-af283499e421/vcfs',
      'age67_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/54535cd7-afbc-4e2c-b36b-636402616606/vcfs',
      'age67_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/004309c3-47de-431e-a716-a0e550411a99/vcfs',
      'age68_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/787a43d6-ce3a-4e17-ba4c-43be2de86f54/vcfs',
      'age68_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/c39a5690-3de8-453a-b3ce-c16bc37fda08/vcfs',
      'age69_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/330667bf-8da5-495b-bd64-353eedaf0045/vcfs',
      'age69_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/dc4cde3c-3412-44aa-8cb7-e15f3d3a6b02/vcfs',
      'age70_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/b5f1526c-2077-404f-b3ed-b9ff6a8585b3/vcfs',
      'age70_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/e5ec5907-3f87-4e3a-9a63-612e80e5e596/vcfs',
      'age71_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/da972edd-d2d4-4f80-a5ac-8fd504251284/vcfs',
      'age71_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/581a7771-2303-402c-8527-817366b1caad/vcfs',
      'age72_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/e18101a2-7929-4465-afba-dd1f8d1f4f5a/vcfs',
      'age72_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/8def03f9-c36a-42eb-b35d-911c59c9a2ea/vcfs',
      'age73_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/82a8972e-ea5d-4d61-b3e5-3e3815017db3/vcfs',
      'age73_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/cd888795-e69d-4737-9e2a-39538551eeee/vcfs',
      'age74_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/b895ab2e-1223-4dc3-a4ea-576402e48ad5/vcfs',
      'age74_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/6b9454ff-0815-40d5-b82b-9301cbfa1732/vcfs',
      'age75_f':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/c7855171-8cd4-4a71-9a12-5573d1d6eefe/vcfs',
      'age75_m':'gs://fc-secure-dc8ec7aa-07c8-4f55-b493-08e0743b3b4c/genomic-extractions/79d94d2c-97a2-4cae-bfec-06d3da66bccf/vcfs',
}

import pickle

with open('../dataset_path.pkl', 'wb') as f:
    pickle.dump(dictt, f)

