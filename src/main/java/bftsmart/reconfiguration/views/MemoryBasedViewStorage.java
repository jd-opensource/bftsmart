package bftsmart.reconfiguration.views;

public class MemoryBasedViewStorage implements ViewStorage{
	
	private volatile View view;
	
	public MemoryBasedViewStorage() {
	}
	
	public MemoryBasedViewStorage(View initView) {
		this.view = initView;
	}

	@Override
	public boolean storeView(View view) {
		this.view = view;
		return true;
	}

	@Override
	public View readView() {
		return view;
	}

}
